package de.skyrising.guardian.gen.mappings

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.annotations.SerializedName
import de.skyrising.guardian.gen.*
import java.net.URI
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.regex.Pattern

val JARS_MAPPED_DIR: Path = JARS_DIR.resolve("mapped")

interface MappingProvider {
    val name: String
    val format: MappingsParser
    fun getVersion(): String = "latest"
    fun getMappings(version: VersionInfo, mappings: String?, target: MappingTarget, cache: Path = CACHE_DIR.resolve("mappings")): CompletableFuture<MappingTree?>
    fun supportsVersion(version: VersionInfo, target: MappingTarget, cache: Path = CACHE_DIR.resolve("mappings")): CompletableFuture<Boolean> {
        return getLatestMappings(version, target, cache).thenApply { it != null }
    }
    fun getPath(cache: Path, version: VersionInfo, mappings: String?): Path = cache.resolve(name).resolve(version.id)
    fun getLatestMappingVersion(version: VersionInfo, target: MappingTarget, cache: Path = CACHE_DIR.resolve("mappings")): CompletableFuture<String?>
    fun getLatestMappings(version: VersionInfo, target: MappingTarget, cache: Path = CACHE_DIR.resolve("mappings")): CompletableFuture<MappingTree?> {
        return getLatestMappingVersion(version, target, cache).thenCompose { mv ->
            getMappings(version, mv, target, cache)
        }
    }

    fun canSkipFinishedMappedJar(cache: Path, version: VersionInfo): Boolean {
        return true
    }

    companion object {
        val MOJANG = object : CommonMappingProvider("mojang", ProguardMappings, "txt", "official") {
            override fun getUrl(cache: Path, version: VersionInfo, mappings: String?, target: MappingTarget): CompletableFuture<URI?> =
                if (target == MappingTarget.MERGED) CompletableFuture.completedFuture(null)
                else getMojangVersionManifest(version).thenApply { manifest ->
                    manifest["downloads"]?.asJsonObject?.get(target.id + "_mappings")?.asJsonObject?.get("url")?.asString?.let { URI(it) }
                }
        }
        val FABRIC_INTERMEDIARY = IntermediaryMappingProvider("fabric", URI("https://meta.fabricmc.net/v2/"), URI("https://maven.fabricmc.net/"))
        val LEGACY_INTERMEDIARY = IntermediaryMappingProvider("legacy", URI("https://meta.legacyfabric.net/v2/"), URI("https://maven.legacyfabric.net/"))
        val QUILT_INTERMEDIARY = IntermediaryMappingProvider("quilt", URI("https://meta.quiltmc.org/v3/"), URI("https://maven.quiltmc.org/repository/release/"))
        val YARN = YarnMappingProvider(URI("https://meta.fabricmc.net/v2/"), URI("https://maven.fabricmc.net/"))
    }
}

abstract class CommonMappingProvider(override val name: String, override val format: MappingsParser, private val ext: String, private val invert: String? = null) : MappingProvider {
    abstract fun getUrl(cache: Path, version: VersionInfo, mappings: String?, target: MappingTarget): CompletableFuture<URI?>
    override fun supportsVersion(version: VersionInfo, target: MappingTarget, cache: Path): CompletableFuture<Boolean> = getLatestMappingVersion(version, target, cache).thenCompose { mappings ->
        getUrl(getPath(cache, version, mappings), version, mappings, target).thenApply { it != null }
    }
    override fun getLatestMappingVersion(version: VersionInfo, target: MappingTarget, cache: Path): CompletableFuture<String?> = CompletableFuture.completedFuture(null)
    override fun getMappings(version: VersionInfo, mappings: String?, target: MappingTarget, cache: Path): CompletableFuture<MappingTree?> {
        val mappingsFile = getPath(cache, version, mappings).resolve("mappings-${target.id}.${ext}")
        return getUrl(getPath(cache, version, mappings), version, mappings, target).thenCompose { url ->
            if (url == null) CompletableFuture.completedFuture(Unit) else download(url, mappingsFile)
        }.thenCompose {
            if (!Files.exists(mappingsFile)) return@thenCompose CompletableFuture.completedFuture(null)
            supplyAsync(TaskType.READ_MAPPINGS) {
                val tree = Files.newBufferedReader(mappingsFile).use(format::parse)
                if (invert != null) Timer(
                    version.id,
                    "mappings.invert.${target.id}"
                ).use { tree.invert(invert) } else tree
            }
        }
    }

    override fun toString() = "${javaClass.simpleName}($name)"
}

abstract class JarMappingProvider(override val name: String, override val format: MappingsParser) : CommonMappingProvider(name, format, "jar") {
    open fun getFile(version: VersionInfo, mappings: String?, target: MappingTarget, jar: FileSystem): Path = jar.getPath("mappings/mappings.tiny")
    override fun getMappings(version: VersionInfo, mappings: String?, target: MappingTarget, cache: Path): CompletableFuture<MappingTree?> {
        val jarFile = getPath(cache, version, mappings).resolve("mappings-${target.id}.jar")
        return getUrl(getPath(cache, version, mappings), version, mappings, target).thenCompose { url ->
            if (url == null) CompletableFuture.completedFuture(Unit) else download(url, jarFile)
        }.thenApplyAsync {
            if (!Files.exists(jarFile)) return@thenApplyAsync null
            getJarFileSystem(jarFile).use { fs ->
                Files.newBufferedReader(getFile(version, mappings, target, fs)).use(format::parse)
            }
        }
    }
}

class IntermediaryMappingProvider(prefix: String, private val meta: URI, private val maven: URI) : JarMappingProvider("$prefix-intermediary", GenericTinyReader) {
    override fun getUrl(cache: Path, version: VersionInfo, mappings: String?, target: MappingTarget): CompletableFuture<URI?> {
        if (target != MappingTarget.MERGED) return CompletableFuture.completedFuture(null)
        return requestJson<JsonArray>(meta.resolve("versions/intermediary/${version.id}")).handle { it, e ->
            if (e != null) null else it
        }.thenApply {
            if (it == null || it.size() == 0) return@thenApply null
            val spec: ArtifactSpec = GSON.fromJson(it[0].asJsonObject["maven"])
            MavenArtifact(maven, spec.copy(classifier = "v2")).getURL()
        }
    }
}

data class MappingMetadata(
    @SerializedName("name") val name: String,
    @SerializedName("version") val version: String,
)

fun readMappingMetadata(file: Path): MappingMetadata {
    return Files.newBufferedReader(file).use { reader ->
        return@use Gson().fromJson(reader, MappingMetadata::class.java)
    }
}

fun writeMappingMetadata(metadata: MappingMetadata, file: Path) {
    Files.newBufferedWriter(file).use { writer ->
        val gson = GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()
        gson.toJson(metadata, writer)
    }
}

class YarnMappingProvider(private val meta: URI, private val maven: URI) : CommonMappingProvider("yarn", GenericTinyReader, "jar") {
    private fun getMetadataJsonDstFile(cache: Path, version: VersionInfo, mappings: String?): Path = getPath(cache, version, mappings).resolve("mappings_metadata.json")
    private fun getCommentJsonDstFile(cache: Path, version: VersionInfo, mappings: String?): Path = getPath(cache, version, mappings).resolve("mappings_comments.json")
    private fun getMappingFileInSrcJar(jar: FileSystem): Path = jar.getPath("mappings/mappings.tiny")
    private var mappingVersion: String? = null
    private var allYarnVersions: CompletableFuture<Set<String>?>? = null
    private val allYarnVersionsLock = Unit

    override fun getVersion(): String {
        return this.mappingVersion ?: "unknown"
    }

    override fun supportsVersion(version: VersionInfo, target: MappingTarget, cache: Path): CompletableFuture<Boolean> {
        synchronized(allYarnVersionsLock) {
            if (allYarnVersions == null) {
                allYarnVersions = requestJson<JsonArray>(meta.resolve("versions/game/yarn")).handle { it, e ->
                    if (e != null) throw e
                    it
                }.thenApply {
                    if (it == null || it.size() == 0) return@thenApply null
                    val versions = mutableSetOf<String>()
                    it.forEach { item -> versions.add(item.asJsonObject.get("version").asString) }
                    return@thenApply versions
                }
            }
        }
        return allYarnVersions!!.thenApply { versions ->
            if (versions == null) return@thenApply null
            return@thenApply versions.contains(version.id)
        }
    }

    override fun getUrl(cache: Path, version: VersionInfo, mappings: String?, target: MappingTarget): CompletableFuture<URI?> {
        return this.getTinyMavenArtifact(cache, version, mappings, target, "yarn").thenApply { artifact -> artifact?.getURL() }
    }

    override fun getMappings(version: VersionInfo, mappings: String?, target: MappingTarget, cache: Path): CompletableFuture<MappingTree?> {
        data class VersionedMappingTree(val ver: String, var mt: MappingTree)
        fun getTinyMapping(what: String) : CompletableFuture<VersionedMappingTree?> {
            val jarFile = getPath(cache, version, mappings).resolve("${what}-${target.id}.jar")
            return getTinyMavenArtifact(getPath(cache, version, mappings), version, mappings, target, what).thenCompose { artifact ->
                if (artifact == null) {
                    return@thenCompose CompletableFuture.completedFuture(null)
                }
                download(artifact.getURL(), jarFile).thenApply {
                    if (!Files.exists(jarFile)) return@thenApply null
                    val mappingTree = getJarFileSystem(jarFile).use { fs ->
                        Files.newBufferedReader(getMappingFileInSrcJar(fs)).use(format::parse)
                    }
                    VersionedMappingTree(artifact.artifact.version, mappingTree)
                }
            }
        }
        val intermediaryMappings = getTinyMapping("intermediary")
        val yarnMappings = getTinyMapping("yarn")
        return CompletableFuture.allOf(yarnMappings, intermediaryMappings).thenApply {
            val im = intermediaryMappings.get() ?: return@thenApply null
            val ym = yarnMappings.get() ?: return@thenApply null

            ym.mt = fixInvertedYarn(ym.mt)

            val commentJsonFile = getCommentJsonDstFile(cache, version, mappings)
            dumpCommentFromYarnMappingTree(ym.mt, "named", commentJsonFile)
            writeMappingMetadata(MappingMetadata("yarn", ym.ver), getMetadataJsonDstFile(cache, version, mappings))
            this.updateMappingInfo(cache, version)

            CombinedYarnMappingTree(im.mt, ym.mt)
        }
    }

    private fun fixInvertedYarn(mappingTree: MappingTree): MappingTree {
        // at least yarn for mc1.14 ~ 1.14.2, the mapping values are inverted
        // so here's a fix
        val idxI = mappingTree.namespaces.indexOf("intermediary")
        val idxN = mappingTree.namespaces.indexOf("named")
        if (idxI == -1 || idxN == -1) {
            throw RuntimeException("unexpected yarn mapping namespaces: ${mappingTree.namespaces}")
        }
        val rawMethodPattern = Pattern.compile("^method_\\d+$")
        var sampleCnt = 0
        mappingTree.classes.forEach check@ { cm ->
            cm.methods.forEach { mm ->
                val intermediaryName = mm.getName(idxI)
                val namedName = mm.getName(idxN)
                if (namedName != intermediaryName && rawMethodPattern.matcher(namedName).matches()) {
                    sampleCnt++
                    if (sampleCnt >= 100) {
                        return@check
                    }
                }
            }
        }
        if (sampleCnt >= 100) {
            val newMappingTree = mappingTree.invert("named")
            System.arraycopy(mappingTree.namespaces, 0, newMappingTree.namespaces, 0, mappingTree.namespaces.size)
            return newMappingTree
        }
        return mappingTree
    }

    private fun getTinyMavenArtifact(cache: Path, version: VersionInfo, mappings: String?, target: MappingTarget, what: String): CompletableFuture<MavenArtifact?> {
        if (target != MappingTarget.MERGED) return CompletableFuture.completedFuture(null)
        return requestJson<JsonArray>(meta.resolve("versions/${what}/${version.id}")).handle { it, e ->
            if (e != null) null else it
        }.thenApply {
            if (it == null || it.size() == 0) return@thenApply null
            val spec: ArtifactSpec = GSON.fromJson(it[0].asJsonObject["maven"])
            MavenArtifact(maven, spec.copy(classifier = "v2"))
        }
    }

    private fun updateMappingInfo(cache: Path, version: VersionInfo) {
        synchronized(DecompileTaskJavadocCommentFileHolder.files) {
            DecompileTaskJavadocCommentFileHolder.files[version.id] = getCommentJsonDstFile(cache, version, null)
        }
        this.mappingVersion = readMappingMetadata(getMetadataJsonDstFile(cache, version, null)).version
    }

    override fun canSkipFinishedMappedJar(cache: Path, version: VersionInfo): Boolean {
        // ensure the javadocProviders is created
        // XXX: currently the mappings var is always null
        val commentJsonFile = getCommentJsonDstFile(cache, version, null)
        val metadataJsonFile = getMetadataJsonDstFile(cache, version, null)

        val canSkip = Files.exists(commentJsonFile) && Files.exists(metadataJsonFile)
        if (canSkip) {
            // FIXME: relocate this side effect?
            this.updateMappingInfo(cache, version)
        }
        return canSkip
    }
}

enum class MappingTarget(val id: String) {
    CLIENT("client"), SERVER("server"), MERGED("merged");
}

val MAPPINGS_CACHE_DIR: Path = CACHE_DIR.resolve("mappings")

fun getMappings(provider: MappingProvider, version: VersionInfo, target: MappingTarget, cache: Path = MAPPINGS_CACHE_DIR): CompletableFuture<MappingTree?> {
    return provider.getLatestMappings(version, target, cache).thenCompose {
        if (it != null || target != MappingTarget.MERGED) return@thenCompose CompletableFuture.completedFuture(it)
        val client = provider.getLatestMappings(version, MappingTarget.CLIENT, cache)
        val server = provider.getLatestMappings(version, MappingTarget.SERVER, cache)
        Timer(version.id, "mappings.merge").use {
            CompletableFuture.allOf(client, server).thenApply {
                val c = client.get() ?: return@thenApply null
                val s = server.get() ?: return@thenApply null
                c.merge(s)
            }
        }
    }
}