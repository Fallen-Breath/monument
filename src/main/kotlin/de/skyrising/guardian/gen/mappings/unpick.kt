package de.skyrising.guardian.gen.mappings

import com.google.gson.Gson
import com.google.gson.JsonObject
import de.skyrising.guardian.gen.ArtifactSpec
import de.skyrising.guardian.gen.MavenArtifact
import de.skyrising.guardian.gen.fromJson
import de.skyrising.guardian.gen.getMavenArtifact
import java.net.URI
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
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
    val version = unpickMeta.get("version").asInt
    if (version == 1) {
        val metadata = Gson().fromJson(metaString, UnpickMetaV1::class.java)
        return UnpickMeta(version, metadata, null)

    } else if (version == 2) {
        val metadata = Gson().fromJson(metaString, UnpickMetaV2::class.java)
        return UnpickMeta(version, null, metadata)
    } else {
        return null
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
        val unpickGroup = "net.fabricmc.unpick"
        val unpickVersion = "2.3.1"
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
        private val classLoaders = ConcurrentHashMap<List<URI>, ClassLoader>()

        fun create(libJars: List<URI>): ClassLoader {
            val classLoader = Thread.currentThread().contextClassLoader
            return classLoaders.computeIfAbsent(libJars) {
                UnpickRunnerClassLoader(libJars, classLoader)
            }
        }
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

private fun runUnpickV2(inputJar: Path, outputJar: Path, cp: List<Path>, meta: UnpickMetaV2, data: UnpickData) {
    // not supported yet
}

fun runUnpick(inputJar: Path, outputJar: Path, cp: List<Path>, data: UnpickData) {
    when (data.metadata.version) {
        1 -> runUnpickV1(inputJar, outputJar, cp, data.metadata.v1!!, data)
        2 -> runUnpickV2(inputJar, outputJar, cp, data.metadata.v2!!, data)
        else -> throw RuntimeException("unsupported version ${data.metadata.version}")
    }
}
