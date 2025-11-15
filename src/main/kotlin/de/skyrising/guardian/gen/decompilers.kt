package de.skyrising.guardian.gen

import com.strobel.assembler.metadata.ITypeLoader
import com.strobel.decompiler.DecompilerSettings
import com.strobel.decompiler.PlainTextOutput
import de.skyrising.guardian.gen.mappings.FabricJavadocProviderCreator
import net.fabricmc.fernflower.api.IFabricJavadocProvider
import org.benf.cfr.reader.api.CfrDriver
import org.jetbrains.java.decompiler.main.Fernflower
import org.jetbrains.java.decompiler.main.decompiler.ConsoleDecompiler
import org.jetbrains.java.decompiler.main.decompiler.DirectoryResultSaver
import org.jetbrains.java.decompiler.main.extern.IFernflowerLogger
import org.jetbrains.java.decompiler.main.extern.IFernflowerPreferences
import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.net.URI
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
import kotlin.io.path.deleteIfExists
import kotlin.io.path.extension

interface Decompiler {
    val name: String
    val maxParallelism get() = Integer.MAX_VALUE
    fun decompile(artifacts: List<MavenArtifact>, version: String, jar: Path, outputDir: (Boolean) -> Path, cp: List<Path>? = null, listener: (String,Boolean) -> Unit = { _, _ -> }): CompletableFuture<Path>

    companion object {
        val CFR = JavaDecompiler("cfr", "de.skyrising.guardian.gen.CfrDecompileTask") {
            for ((_, artifact) in it) {
                if (artifact.id != "cfr") continue
                val version = artifact.version.split('.')
                // Workaround for https://github.com/leibnitz27/cfr/issues/250 below 0.152
                if (version.size == 2 && version[0] == "0" && version[1].toInt() < 152) {
                    return@JavaDecompiler false
                }
            }
            true
        }
        val FERNFLOWER = JavaDecompiler("fernflower", "de.skyrising.guardian.gen.FernflowerDecompileTask")
        val FORGEFLOWER = JavaDecompiler("forgeflower", "de.skyrising.guardian.gen.FernflowerDecompileTask")
        val FABRIFLOWER = JavaDecompiler("fabriflower", "de.skyrising.guardian.gen.FernflowerDecompileTask")
        val QUILTFLOWER = JavaDecompiler("quiltflower", "de.skyrising.guardian.gen.FernflowerDecompileTask")
        val VINEFLOWER = JavaDecompiler("vineflower", "de.skyrising.guardian.gen.VineflowerDecompileTask")
        val PROCYON = JavaDecompiler("procyon", "de.skyrising.guardian.gen.ProcyonDecompileTask")
    }
}

@Suppress("unused")
class CfrDecompileTask : DecompileTask {
    override fun decompile(version: String, jar: Path, outputDir: (Boolean) -> Path, cp: List<Path>?, listener: (String,Boolean) -> Unit): Path {
        val outDir = outputDir(false)
        Timer(version, "decompile").use {
            val options = mutableMapOf(
                "showversion" to "false",
                "silent" to "false",
                "comments" to "false",
                "outputpath" to outDir.toAbsolutePath().toString()
            )
            if (cp != null && cp.isNotEmpty()) options["extraclasspath"] = formatClassPath(cp)
            val driver = CfrDriver.Builder().withOptions(options).build()
            listen({ line ->
                val cls = line.substringAfter("Processing ", "")
                if (cls.isNotEmpty()) listener(cls, false)
            }) {
                driver.analyse(listOf(jar.toAbsolutePath().toString()))
            }
        }
        return outDir
    }
}

abstract class CommonDecompiler(override val name: String) : Decompiler {
    override fun toString() = "CommonDecompiler($name)"
}

class JavaDecompiler(name: String, private val taskClassName: String, private val allowSharing: (List<MavenArtifact>) -> Boolean = { true }) : CommonDecompiler(name) {
    private val classLoaders = ConcurrentHashMap<List<URI>, ClassLoader>()

    override fun decompile(
        artifacts: List<MavenArtifact>,
        version: String,
        jar: Path,
        outputDir: (Boolean) -> Path,
        cp: List<Path>?,
        listener: (String,Boolean) -> Unit
    ): CompletableFuture<Path> = getMavenArtifacts(artifacts).thenCompose { urls: List<URI> ->
        supplyAsync(TaskType.DECOMPILE) {
            outputTo(version) {
                val classLoader = if (allowSharing(artifacts)) {
                    classLoaders.computeIfAbsent(urls, this::createClassLoader)
                } else {
                    println("Creating a new class loader because a decompiler issue prevents it from being reused")
                    val executor = threadLocalContext.get().executor as CustomThreadPoolExecutor
                    executor.decompileParallelism = minOf(executor.decompileParallelism, 4)
                    createClassLoader(urls)
                }
                val task = Class.forName(taskClassName, true, classLoader).newInstance()
                if (task.javaClass.classLoader != classLoader) {
                    throw IllegalStateException("$taskClassName loaded by incorrect class loader: ${task.javaClass.classLoader}")
                }
                if (task !is DecompileTask) throw IllegalStateException("$task is not a DecompileTask")
                task.decompile(version, jar, outputDir, cp, listener)
            }
        }
    }

    private fun createClassLoader(urls: List<URI>) = DecompileTaskClassLoader(URLClassLoader(arrayOf(
        getBaseUrl(JavaDecompiler::class.java),
        *urls.map(URI::toURL).toTypedArray()
    )))

    override fun toString() = "JavaDecompiler($name)"
}

class DecompileTaskClassLoader(parent: ClassLoader) : ClassLoader(parent) {
    override fun loadClass(name: String, resolve: Boolean): Class<*> {
        if (!shouldLoad(name)) {
            return super.loadClass(name, resolve)
        }
        synchronized(getClassLoadingLock(name)) {
            var c = findLoadedClass(name)
            if (c == null) {
                val path = name.replace('.', '/') + ".class"
                val stream = parent.getResourceAsStream(path)
                c = if (stream == null) {
                    super.loadClass(name, false)
                } else {
                    val bytes = stream.readBytes()
                    defineClass(name, bytes, 0, bytes.size)
                }
            }
            if (c == null) throw ClassNotFoundException(name)
            if (resolve) resolveClass(c)
            return c
        }
    }

    private fun shouldLoad(name: String): Boolean {
        if (!name.startsWith("de.skyrising.guardian.gen.")) return false
        val outerClass = name.substringBefore('$')
        if (outerClass == DecompileTask::class.java.name) return false
        return outerClass.endsWith("DecompileTask") || outerClass.endsWith("FabricJavadocProviderCreator")
    }
}

private fun getBaseUrl(cls: Class<*>): URL {
    val path = "/" + cls.name.replace('.', '/') + ".class"
    val url = cls.getResource(path)?.toExternalForm() ?: throw FileNotFoundException("Can't find $path")
    if (url.startsWith("jar:")) {
        return URL(url.substring(4, url.indexOf("!/")))
    }
    return URL(url.take(url.lastIndexOf(path)))
}

interface DecompileTask {
    fun decompile(version: String, jar: Path, outputDir: (Boolean) -> Path, cp: List<Path>?, listener: (String,Boolean) -> Unit): Path
}

@Suppress("unused")
open class FernflowerDecompileTask : DecompileTask {
    protected fun getArgs(jar: Path, outputDir: Path, cp: List<Path>?, defaults: Map<String, Any>): Array<String> {
        val args = mutableListOf("-ind=    ")
        if ("jrt" in defaults) {
            args.add("-jrt=1")
        } else if (IFernflowerPreferences.WARN_INCONSISTENT_INNER_CLASSES in defaults) {
            // QuiltFlower has fast iec
            args.add("-iec=1")
        }
        args.add("-rsy=1")
        args.add("-dgs=1")
        if (IFernflowerPreferences.THREADS in defaults) {
            val executor = threadLocalContext.get().executor as CustomThreadPoolExecutor
            val maxTotalThreads = maxOf(executor.parallelism - 4, 1)
            val decompilers = executor.decompileParallelism
            val threads = maxOf((maxTotalThreads + decompilers - 1) / decompilers, 1)
            args.add("-thr=$threads")
        }
        if ("pam" in defaults) {
            args.add("-pam=1")
        }
        //val executor = threadLocalContext.get().executor as CustomThreadPoolExecutor
        //executor.decompileParallelism = minOf(4, MAX_THREADS)
        if (cp != null) {
            for (p in cp) args.add("-e=${p.toAbsolutePath()}")
        }
        args.add(jar.toAbsolutePath().toString())
        args.add(outputDir.toAbsolutePath().toString())
        return args.toTypedArray()
    }

    override fun decompile(version: String, jar: Path, outputDir: (Boolean) -> Path, cp: List<Path>?, listener: (String,Boolean) -> Unit): Path {
        val outDir = outputDir(false)
        val clsOutput = outDir.resolve("bin")
        val srcOutput = outDir.resolve("src")
        Timer(version, "decompile.extractClasses").use {
            getJarFileSystem(jar).use {
                copy(it.getPath("/"), clsOutput) { p -> p.extension == "class" }
            }
            Files.createDirectories(srcOutput)
        }
        Timer(version, "decompile").use {
            listen({ line ->
                var cls = line.substringAfter("Decompiling class ", "")
                var preprocessing = false
                if (cls.isEmpty()) {
                    cls = line.substringAfter("Preprocessing class ", "")
                    preprocessing = true
                }
                if (cls.isNotEmpty()) listener(cls, preprocessing)
            }) {
                ConsoleDecompiler.main(getArgs(clsOutput, srcOutput, cp, IFernflowerPreferences.DEFAULTS))
            }
        }
        return srcOutput
    }
}

class DecompileTaskExtraFeatureHolder {
    companion object {
        val comments = ConcurrentHashMap<String, Path>()  // version -> path to the comments.json
    }
}

@Suppress("unused")
open class VineflowerDecompileTask : DecompileTask {
    private fun createClassOnlyJar(srcJar: Path, dstJar: Path) {
        ZipInputStream(FileInputStream(srcJar.toFile())).use { zis ->
            ZipOutputStream(FileOutputStream(dstJar.toFile())).use { zos ->
                var entry = zis.nextEntry
                while (entry != null) {
                    if (!entry.isDirectory && entry.name.endsWith(".class")) {
                        zos.putNextEntry(ZipEntry(entry.name))
                        zis.copyTo(zos)
                        zos.closeEntry()
                    }
                    entry = zis.nextEntry
                }
            }
        }
    }

    override fun decompile(version: String, jar: Path, outputDir: (Boolean) -> Path, cp: List<Path>?, listener: (String,Boolean) -> Unit): Path {
        val outDir = outputDir(false)
        val srcOutput = outDir.resolve("src")
        val classOnlyJar = outDir.resolve("src.jar")
        createClassOnlyJar(jar, classOnlyJar)

        val javadocCommentFile = DecompileTaskExtraFeatureHolder.comments[version]

        val executor = threadLocalContext.get().executor as CustomThreadPoolExecutor
        val maxTotalThreads = maxOf(executor.parallelism - 4, 1)
        val decompilers = executor.decompileParallelism
        val threads = maxOf((maxTotalThreads + decompilers - 1) / decompilers, 1)
        val options = mapOf(
            IFernflowerPreferences.INDENT_STRING to "    ",
            IFernflowerPreferences.INCLUDE_JAVA_RUNTIME to "1",
            IFernflowerPreferences.INCLUDE_ENTIRE_CLASSPATH to "1",
            IFernflowerPreferences.REMOVE_SYNTHETIC to "1",
            IFernflowerPreferences.DECOMPILE_GENERIC_SIGNATURES to "1",
            IFernflowerPreferences.THREADS to threads.toString(),
            IFernflowerPreferences.PATTERN_MATCHING to "1",
            IFabricJavadocProvider.PROPERTY_NAME to FabricJavadocProviderCreator.createFabricJavadocProviderFromJson(javadocCommentFile),
        )

        Timer(version, "decompile").use {
            val saver = DirectoryResultSaver(srcOutput.toFile())
            val ff = Fernflower(saver, options, object : IFernflowerLogger() {
                // see org.jetbrains.java.decompiler.main.decompiler.PrintStreamLogger
                override fun startReadingClass(className: String?) {
                    if (className != null) {
                        output(version, "Decompiling class $className")
                        listener(className, false)
                    }
                }

                override fun startProcessingClass(className: String?) {
                    if (className != null) {
                        output(version, "Preprocessing class $className")
                        listener(className, true)
                    }
                }

                override fun writeMessage(message: String?, severity: Severity?) {
                }

                override fun writeMessage(message: String?, severity: Severity?, t: Throwable?) {
                }
            })

            if (cp != null) {
                for (p in cp) ff.addLibrary(p.toFile())
            }
            ff.addSource(classOnlyJar.toFile())

            try {
                ff.decompileContext()
            } finally {
                ff.clearContext()
            }
            classOnlyJar.deleteIfExists()
            output(version, "Decompilation completed")
        }
        return srcOutput
    }
}

private fun formatClassPath(cp: List<Path>) = cp.joinToString(File.pathSeparator) { it.toAbsolutePath().toString() }

@Suppress("unused")
class ProcyonDecompileTask : DecompileTask {
    override fun decompile(version: String, jar: Path, outputDir: (Boolean) -> Path, cp: List<Path>?, listener: (String,Boolean) -> Unit): Path {
        val outDir = outputDir(true)
        val executor = threadLocalContext.get().executor as CustomThreadPoolExecutor
        executor.decompileParallelism = minOf(
            MAX_THREADS,
            (Runtime.getRuntime().maxMemory() / (768 * 1024 * 1024)).toInt()
        )
        Timer(version, "decompile").use {
            getJarFileSystem(jar).use {
                val settings = DecompilerSettings.javaDefaults()
                settings.isUnicodeOutputEnabled = true
                settings.typeLoader = ITypeLoader { name, buffer ->
                    val path = it.getPath("$name.class")
                    if (!Files.exists(path)) return@ITypeLoader false
                    val bytes = Files.readAllBytes(path)
                    buffer.putByteArray(bytes, 0, bytes.size)
                    buffer.position(0)
                    true
                }
                val errors = TreeMap<String, Throwable>()
                Files.walk(it.rootDirectories.first())
                    .map(Path::toString)
                    .filter { str -> str.endsWith(".class") && !str.contains('$') }
                    .forEach { p ->
                        val internalName = p.substring(1, p.length - 6)
                        println("Decompiling ${internalName.replace('/', '.')}")
                        val outPath = outDir.resolve("$internalName.java")
                        try {
                            Files.createDirectories(outPath.parent)
                            Files.newBufferedWriter(outPath).use { outWriter ->
                                com.strobel.decompiler.Decompiler.decompile(
                                    internalName,
                                    PlainTextOutput(outWriter),
                                    settings
                                )
                            }
                        } catch (t: Throwable) {
                            errors[internalName] = t
                        }
                        listener(internalName, false)
                    }
                if (errors.isNotEmpty()) {
                    val sb = StringBuilder()
                    for ((file, error) in errors.entries) {
                        sb.append(file).append('\n')
                        sb.append(error.message).append('\n')
                    }
                    Files.write(outDir.resolve("errors.txt"), sb.toString().toByteArray())
                }
            }
        }
        return outDir
    }
}