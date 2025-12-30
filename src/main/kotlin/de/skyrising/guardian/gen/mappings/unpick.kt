package de.skyrising.guardian.gen.mappings

import com.google.gson.Gson
import com.google.gson.JsonObject
import daomephsta.unpick.api.ConstantUninliner
import daomephsta.unpick.api.classresolvers.ClassResolvers
import daomephsta.unpick.api.classresolvers.IClassResolver
import daomephsta.unpick.api.constantgroupers.ConstantGroupers
import daomephsta.unpick.constantmappers.datadriven.parser.v3.UnpickV3Reader
import daomephsta.unpick.constantmappers.datadriven.parser.v3.UnpickV3Remapper
import daomephsta.unpick.constantmappers.datadriven.parser.v3.UnpickV3Writer
import de.skyrising.guardian.gen.*
import org.objectweb.asm.ClassReader
import org.objectweb.asm.ClassWriter
import org.objectweb.asm.Type
import org.objectweb.asm.tree.ClassNode
import java.io.*
import java.net.URI
import java.net.URLClassLoader
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
import kotlin.io.path.absolute
import kotlin.io.path.toPath

data class UnpickMetaV1(val unpickGroup: String, val unpickVersion: String)
data class UnpickMetaV2(val namespace: String, val constants: String? = null)
data class UnpickMeta(val version: Int, val v1: UnpickMetaV1?, val v2: UnpickMetaV2?)
data class UnpickData(
    val metadata: UnpickMeta,
    val unpickJarPaths: List<String>,
    val definitionsPath: String,
    val constantJarPath: String?
)

fun readUnpickMeta(unpickMetaPath: Path): UnpickMeta? {
    val metaString = Files.newBufferedReader(unpickMetaPath).use { it.readText() }
    val unpickMeta = Gson().fromJson<JsonObject>(metaString)
    when (val version = unpickMeta.get("version").asInt) {
        1 -> {
            val metadata = Gson().fromJson(metaString, UnpickMetaV1::class.java)
            return UnpickMeta(version, metadata, null)
        }
        2 -> {
            val metadata = Gson().fromJson(metaString, UnpickMetaV2::class.java)
            return UnpickMeta(version, null, metadata)
        }
        else -> {
            return null
        }
    }
}

fun downloadUnpickLib(mavenUrl: URI, metadata: UnpickMeta): CompletableFuture<List<Path>> {
    fun downloadArtifacts(vararg specs: ArtifactSpec): CompletableFuture<List<Path>> {
        val futures = specs.map {
            getMavenArtifact(MavenArtifact(mavenUrl, it))
        }
        return CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
            futures.map { it.get().toPath() }
        }
    }
    if (metadata.v1 != null) {
        // always use fabric's latest unpick v2, instead of the one in unpick meta v1
        val useFabricLatestUnpickV2 = true
        val unpickGroup = if (useFabricLatestUnpickV2) "net.fabricmc.unpick" else metadata.v1.unpickGroup
        val unpickVersion = if (useFabricLatestUnpickV2) "2.3.1" else metadata.v1.unpickVersion
        return downloadArtifacts(
            ArtifactSpec(unpickGroup, "unpick", unpickVersion),
            ArtifactSpec(unpickGroup, "unpick-format-utils", unpickVersion),
            ArtifactSpec(unpickGroup, "unpick-cli", unpickVersion),
        )
    } else if (metadata.v2 != null) {
        val unpickGroup = "net.fabricmc.unpick"
        val unpickVersion = "3.0.0-beta.13"
        return downloadArtifacts(
            ArtifactSpec(unpickGroup, "unpick", unpickVersion),
            ArtifactSpec(unpickGroup, "unpick-format-utils", unpickVersion),
        )
    } else {
        throw IllegalArgumentException("bad metadata $metadata")
    }
}

class UnpickRunnerClassLoader(libJars: List<URI>, parent: ClassLoader) :
    URLClassLoader(libJars.map { it.toURL() }.toTypedArray(), parent) {

    companion object {
        fun create(libJars: List<URI>): ClassLoader {
            val classLoader = Thread.currentThread().contextClassLoader
            return UnpickRunnerClassLoader(libJars, classLoader)
        }
    }

    override fun loadClass(name: String, resolve: Boolean): Class<*> {
        if (!shouldLoad(name)) {
            return super.loadClass(name, resolve)
        }
        synchronized(getClassLoadingLock(name)) {
            var c = findLoadedClass(name)
            if (c == null) {
                c = loadClassFromParent(name)
            }
            if (c == null) throw ClassNotFoundException(name)
            if (resolve) resolveClass(c)
            return c
        }
    }

    private fun loadClassFromParent(name: String): Class<*>? {
        val path = name.replace('.', '/') + ".class"
        val stream = parent.getResourceAsStream(path)
        return if (stream == null) {
            super.loadClass(name, false)
        } else {
            val bytes = stream.readBytes()
            defineClass(name, bytes, 0, bytes.size)
        }
    }

    private fun shouldLoad(name: String): Boolean {
        if (!name.startsWith("de.skyrising.guardian.gen.mappings.")) return false
        val outerClass = name.substringBefore('$')
        return outerClass.endsWith("UnpickV2Runner") || outerClass.endsWith("NullLogHandler")
    }
}

private class NullLogHandler() : Handler() {
    override fun publish(record: LogRecord?) {
    }

    override fun flush() {
    }

    override fun close() {
    }
}

private fun runUnpickV1(inputJar: Path, outputJar: Path, cp: List<Path>, meta: UnpickMetaV1, data: UnpickData) {
    val logger = Logger.getLogger("unpick")
    logger.setUseParentHandlers(false)
    logger.addHandler(NullLogHandler())

    val argPaths = mutableListOf<Path>()
    argPaths.add(inputJar)
    argPaths.add(outputJar)
    argPaths.add(Path.of(data.definitionsPath))
    argPaths.add(Path.of(data.constantJarPath!!)) // should not be null for v1
    argPaths.addAll(cp)
    val args = argPaths.map { it.toFile().absolutePath }.toTypedArray()

    val classLoader = UnpickRunnerClassLoader.create(data.unpickJarPaths.map { Path.of(it).absolute().toUri() })
    try {
        val clazz = Class.forName("daomephsta.unpick.cli.Main", true, classLoader)
        val method = clazz.getMethod("main", Array<String>::class.java)
        method.invoke(null, args)
    } catch (e: Exception) {
        throw RuntimeException("Failed to run unpick", e)
    }
}

data class UnpickV2RunnerArgs(
    val inputJar: Path,
    val outputJar: Path,
    val mcLibs: List<Path>,
    val meta: UnpickMetaV2,
    val data: UnpickData,
    val mappings: CombinedYarnMappingTree,
    val mcLibsFsMap: Map<Path, FileSystem>,
)

@Suppress("unused")
open class UnpickV2Runner {
    fun run(args: UnpickV2RunnerArgs) {
        val logger = Logger.getLogger("unpick")
        logger.setUseParentHandlers(false)
        logger.addHandler(NullLogHandler())

        val unpickNonMcLibPaths = mutableListOf<Path>()
        args.data.constantJarPath?.let { unpickNonMcLibPaths.add(Path.of(it)) }
        unpickNonMcLibPaths.add(args.inputJar)

        val unpickClassPaths = mutableListOf<Path>()
        unpickClassPaths.addAll(args.mcLibs)
        unpickClassPaths.addAll(unpickNonMcLibPaths)

        var unpickDefContent = Files.newBufferedReader(Path.of(args.data.definitionsPath)).use { it.readText() }
        if (args.meta.namespace != "named") {
            if (args.meta.namespace != "intermediary") {
                throw IllegalArgumentException("Unsupported unpick definition namespace '${args.meta.namespace}'")
            }
            val reader = UnpickV3Reader(StringReader(unpickDefContent))
            val writer = UnpickV3Writer()
            val yarn = args.mappings.yarn
            val destNamespace = yarn.namespaces.size - 1

            val mcAndItsLibClassLoader = URLClassLoader(unpickClassPaths.map { it.toUri().toURL() }.toTypedArray())

            reader.accept(object : UnpickV3Remapper(writer) {
                override fun mapClassName(className: String): String {
                    return yarn.mapType(className.replace('.', '/'), destNamespace)
                        ?.replace('/', '.') ?: className
                }

                override fun mapFieldName(className: String, fieldName: String, fieldDesc: String): String {
                    val clz = yarn.classes[className.replace('.', '/')] ?: return fieldName
                    return clz.fields[MemberDescriptor(fieldName, fieldDesc)]?.getName(destNamespace) ?: fieldName
                }

                override fun mapMethodName(className: String, methodName: String, methodDesc: String): String {
                    val clz = yarn.classes[className.replace('.', '/')] ?: return methodName
                    return clz.methods[MemberDescriptor(methodName, methodDesc)]?.getName(destNamespace) ?: methodName
                }

                override fun getClassesInPackage(pkg: String): List<String> {
                    TODO("Not yet implemented: $pkg")
                }

                override fun getFieldDesc(className: String, fieldName: String): String {
                    val yarnResult = yarn.classes[className.replace('.', '/')]?.fields?.firstOrNull { it.defaultName.name == fieldName }?.defaultName
                    if (yarnResult != null) return yarnResult.type
                    val clazz = Class.forName(className, false, mcAndItsLibClassLoader)
                    val field = clazz.getDeclaredField(fieldName)
                    return Type.getDescriptor(field.type)
                }
            })
            unpickDefContent = writer.getOutput().replace(System.lineSeparator(), "\n")

            // NOTE: debug only
//            Files.newBufferedWriter(Path.of("remapped.unpick")).use { it.write(unpickDefContent) }
        }

        getJarFileSystems(unpickNonMcLibPaths).use {
            val fileSystems = mutableListOf<FileSystem>()
            fileSystems.addAll(args.mcLibs.mapNotNull(args.mcLibsFsMap::get))
            fileSystems.addAll(it.fileSystems)

            var resolver: IClassResolver = ClassResolvers.fromDirectory(fileSystems[0].getPath("/"))
            for (i in 1 until unpickClassPaths.size) {
                resolver = resolver.chain(ClassResolvers.fromDirectory(fileSystems[i].getPath("/")))
            }
            resolver = resolver.chain(ClassResolvers.classpath())

            runUnpick(resolver, StringReader(unpickDefContent), args.inputJar.toFile(), args.outputJar.toFile())
        }
    }

    private fun runUnpick(resolver: IClassResolver, mappingSource: Reader, inputFile: File, outputFile: File) {
        val uninliner = ConstantUninliner.builder()
            .classResolver(resolver)
            .grouper(
                ConstantGroupers.dataDriven()
                    .lenient(false)  // value == is meta v1
                    .classResolver(resolver)
                    .mappingSource(mappingSource)
                    .build()
            )
            .build()

        ZipInputStream(FileInputStream(inputFile)).use { zis ->
            ZipOutputStream(FileOutputStream(outputFile)).use { zos ->
                var entry = zis.nextEntry
                while (entry != null) {
                    if (!entry.isDirectory && entry.name.endsWith(".class")) {
                        val classNode = ClassNode()
                        val reader = ClassReader(zis)
                        reader.accept(classNode, 0)
                        uninliner.transform(classNode)
                        val writer = ClassWriter(ClassWriter.COMPUTE_MAXS)
                        classNode.accept(writer)

                        zos.putNextEntry(ZipEntry(entry.name))
                        zos.write(writer.toByteArray())
                        zos.closeEntry()
                    }
                    entry = zis.nextEntry
                }
            }
        }
    }
}

private fun runUnpickV2(inputJar: Path, outputJar: Path, cp: List<Path>, meta: UnpickMetaV2, data: UnpickData, mappings: CombinedYarnMappingTree) {
    val cpFsMap = mutableMapOf<Path, FileSystem>()
    cp.forEach { cpFsMap[it] = OPENED_LIBRARIES.computeIfAbsent(it, ::getJarFileSystem) }

    val classLoader = UnpickRunnerClassLoader.create(data.unpickJarPaths.map { Path.of(it).absolute().toUri() })
    try {
        val clazz = Class.forName("de.skyrising.guardian.gen.mappings.UnpickV2Runner", true, classLoader)
        val obj = clazz.getDeclaredConstructor().newInstance()
        if (obj.javaClass.classLoader != classLoader) {
            throw IllegalStateException("$obj loaded by incorrect class loader: ${obj.javaClass.classLoader}")
        }
        val method = clazz.getMethod("run", UnpickV2RunnerArgs::class.java)
        method.invoke(obj, UnpickV2RunnerArgs(inputJar, outputJar, cp, meta, data, mappings, cpFsMap))
    } catch (e: Exception) {
        throw RuntimeException("Failed to run unpick", e)
    }
}

fun runUnpick(inputJar: Path, outputJar: Path, cp: List<Path>, data: UnpickData, mappings: CombinedYarnMappingTree) {
    when (data.metadata.version) {
        1 -> runUnpickV1(inputJar, outputJar, cp, data.metadata.v1!!, data)
        2 -> runUnpickV2(inputJar, outputJar, cp, data.metadata.v2!!, data, mappings)
        else -> throw RuntimeException("unsupported version ${data.metadata.version}")
    }
}
