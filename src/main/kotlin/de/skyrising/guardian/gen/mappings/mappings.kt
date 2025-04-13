package de.skyrising.guardian.gen.mappings

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.annotations.SerializedName
import de.skyrising.guardian.gen.*
import java.io.FileInputStream
import java.io.InputStream
import java.net.URI
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function
import java.util.regex.Pattern
import java.util.zip.ZipInputStream

val JARS_MAPPED_DIR: Path = JARS_DIR.resolve("mapped")

interface MappingProvider {
    val name: String
    val format: MappingsParser
    fun getVersion(version: VersionInfo): String = "latest"
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
    fun postProcessMergedVersion(version: VersionInfo, mappings: String?, target: MappingTarget, cache: Path, mappingTree: MappingTree?): CompletableFuture<MappingTree?> = CompletableFuture.completedFuture(mappingTree)
    fun postProcessMerged(version: VersionInfo, target: MappingTarget, cache: Path, mappingTree: MappingTree?): CompletableFuture<MappingTree?> {
        return getLatestMappingVersion(version, target, cache).thenCompose { mv ->
            postProcessMergedVersion(version, mv, target, cache, mappingTree)
        }
    }

    companion object {
        val MOJANG = MojangMappingProvider("mojang")
        val FABRIC_INTERMEDIARY = IntermediaryMappingProvider("fabric", URI("https://meta.fabricmc.net/v2/"), URI("https://maven.fabricmc.net/"))
        val LEGACY_INTERMEDIARY = IntermediaryMappingProvider("legacy", URI("https://meta.legacyfabric.net/v2/"), URI("https://maven.legacyfabric.net/"))
        val QUILT_INTERMEDIARY = IntermediaryMappingProvider("quilt", URI("https://meta.quiltmc.org/v3/"), URI("https://maven.quiltmc.org/repository/release/"))
        val YARN = YarnMappingProvider("yarn", URI("https://meta.fabricmc.net/v2/"), URI("https://maven.fabricmc.net/"))
        val LEGACY_YARN = YarnMappingProvider("legacy-yarn", URI("https://meta.legacyfabric.net/v2/"), URI("https://maven.legacyfabric.net/"))
        val PARCHMENT = ParchmentMappingProvider("parchment", URI("https://maven.parchmentmc.org/"))
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

open class MojangMappingProvider(override val name: String) : CommonMappingProvider(name, ProguardMappings, "txt", "official") {
    override fun getUrl(cache: Path, version: VersionInfo, mappings: String?, target: MappingTarget): CompletableFuture<URI?> =
        if (target == MappingTarget.MERGED) CompletableFuture.completedFuture(null)
        else getMojangVersionManifest(version).thenApply { manifest ->
            manifest["downloads"]?.asJsonObject?.get(target.id + "_mappings")?.asJsonObject?.get("url")?.asString?.let { URI(it) }
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

class AdvancedMappingHelper(private val provider: MappingProvider) {
    private val mappingVersions = ConcurrentHashMap<String, String>()  // version.id -> mapping version

    fun getMappingVersion(version: VersionInfo): String {
        return mappingVersions[version.id] ?: "unknown"
    }
    fun setMappingVersion(version: VersionInfo, mappingVersion: String) {
        mappingVersions[version.id] = mappingVersion
    }
    fun getMetadataJsonFile(cache: Path, version: VersionInfo, mappings: String?): Path = provider.getPath(cache, version, mappings).resolve("mappings-metadata.json")
    fun getCommentJsonFile(cache: Path, version: VersionInfo, mappings: String?): Path = provider.getPath(cache, version, mappings).resolve("mappings-comments.json")

    fun writeMetadata(cache: Path, version: VersionInfo, mappings: String?) {
        Files.newBufferedWriter(getMetadataJsonFile(cache, version, mappings)).use { writer ->
            val gson = GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()
            gson.toJson(MappingMetadata(provider.name, getMappingVersion(version)), writer)
        }
    }

    fun writeComment(cache: Path, version: VersionInfo, mappings: String?, mappingTree: MappingTree, namespace: String) {
        dumpCommentFromYarnMappingTree(mappingTree, namespace, getCommentJsonFile(cache, version, mappings))
    }

    fun loadMetadata(cache: Path, version: VersionInfo, mappings: String?) {
        val metadata = Files.newBufferedReader(getMetadataJsonFile(cache, version, mappings)).use { reader ->
            return@use Gson().fromJson(reader, MappingMetadata::class.java)
        }
        if (metadata.name != provider.name) throw RuntimeException("Unmatched mapping provider name, read ${metadata.name}, should be ${provider.name}")
        this.setMappingVersion(version, metadata.version)
    }

    fun registerComment(cache: Path, version: VersionInfo) {
        DecompileTaskJavadocCommentFileHolder.files[version.id] = getCommentJsonFile(cache, version, null)
    }

    fun checkSkipAndLoadIfOk(cache: Path, version: VersionInfo, mappings: String?): Boolean {
        val canSkip = Files.exists(getCommentJsonFile(cache, version, mappings)) && Files.exists(getMetadataJsonFile(cache, version, mappings))
        if (canSkip) {
            // FIXME: relocate this side effect?
            loadMetadata(cache, version, mappings)
            registerComment(cache, version)
        }
        return canSkip
    }
}

class YarnMappingProvider(override val name: String, private val meta: URI, private val maven: URI) : CommonMappingProvider(name, GenericTinyReader, "jar") {
    private val helper = AdvancedMappingHelper(this)
    private fun getMappingFileInSrcJar(jar: FileSystem): Path = jar.getPath("mappings/mappings.tiny")
    private val allYarnVersions: CompletableFuture<Set<String>?> by lazy {
        requestJson<JsonArray>(meta.resolve("versions/game/yarn")).handle { it, e ->
            if (e != null) throw e
            it
        }.thenApply {
            if (it == null || it.size() == 0) return@thenApply null
            val versions = mutableSetOf<String>()
            it.forEach { item -> versions.add(item.asJsonObject.get("version").asString) }
            return@thenApply versions
        }
    }

    override fun getVersion(version: VersionInfo): String = helper.getMappingVersion(version)

    override fun supportsVersion(version: VersionInfo, target: MappingTarget, cache: Path): CompletableFuture<Boolean> {
        return allYarnVersions.thenApply { versions ->
            if (versions == null) {
                throw RuntimeException("allYarnVersions is null")
            }
            return@thenApply versions.contains(version.id)
        }
    }

    override fun getUrl(cache: Path, version: VersionInfo, mappings: String?, target: MappingTarget): CompletableFuture<URI?> {
        return this.getTinyMavenArtifact(version, target, "yarn").thenApply { artifact -> artifact?.getURL() }
    }

    override fun getMappings(version: VersionInfo, mappings: String?, target: MappingTarget, cache: Path): CompletableFuture<MappingTree?> {
        data class VersionedMappingTree(val ver: String, var mt: MappingTree)
        fun getTinyMapping(what: String) : CompletableFuture<VersionedMappingTree?> {
            val jarFile = getPath(cache, version, mappings).resolve("${what}-${target.id}.jar")
            return getTinyMavenArtifact(version, target, what).thenCompose { artifact ->
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

            helper.setMappingVersion(version, ym.ver)
            helper.writeMetadata(cache, version, mappings)
            helper.writeComment(cache, version, mappings, ym.mt, "named")
            helper.registerComment(cache, version)

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

    private fun getTinyMavenArtifact(version: VersionInfo, target: MappingTarget, what: String): CompletableFuture<MavenArtifact?> {
        if (target != MappingTarget.MERGED) return CompletableFuture.completedFuture(null)
        return requestJson<JsonArray>(meta.resolve("versions/${what}/${version.id}")).handle { it, e ->
            if (e != null) output(version.id, "requestJson for $what ${version.id} failed: $e")
            if (e != null) null else it
        }.thenApply {
            if (it == null || it.size() == 0) return@thenApply null
            val spec: ArtifactSpec = GSON.fromJson(it[0].asJsonObject["maven"])
            MavenArtifact(maven, spec.copy(classifier = "v2"))
        }
    }

    override fun canSkipFinishedMappedJar(cache: Path, version: VersionInfo): Boolean {
        // ensure the javadocProviders is created
        // XXX: currently the mappings var is always null
        return helper.checkSkipAndLoadIfOk(cache, version, null)
    }
}

class ParchmentMappingProvider(override val name: String, private val maven: URI) : MojangMappingProvider(name) {
    private val helper = AdvancedMappingHelper(this)
    private val allSupportedVersions: CompletableFuture<Set<String>?> by lazy {
        requestText(maven.resolve("org/parchmentmc/data")).handle { it, e ->
            if (e != null) null else it
        }.thenApply {
            if (it.isNullOrEmpty()) return@thenApply null
            val versions = mutableSetOf<String>()
            val pattern = Pattern.compile("""^\s*<a href="parchment-([0-9a-z.-]+)/">parchment-([0-9a-z.-]+)/</a>.*$""")
            it.lines().forEach { line ->
                val matcher = pattern.matcher(line)
                if (matcher.matches()) {
                    val ver1 = matcher.group(1)
                    val ver2 = matcher.group(2)
                    if (ver1 == ver2 && ver1 != null) {
                        versions.add(ver1)
                    }
                }
            }
            return@thenApply versions
        }
    }

    override fun getVersion(version: VersionInfo): String = helper.getMappingVersion(version)

    override fun supportsVersion(version: VersionInfo, target: MappingTarget, cache: Path): CompletableFuture<Boolean> {
        val allSupportedVersionsFuture = allSupportedVersions
        return super.supportsVersion(version, target, cache).thenApply { superOk ->
            if (!superOk) return@thenApply false
            return@thenApply allSupportedVersionsFuture.get()?.contains(version.id) ?: false
        }
    }

    data class ParchmentParameterMapping(val index: Int, val name: String, val descriptor: String, val javadoc: String?)
    data class ParchmentMethodMapping(val name: String, val descriptor: String, val javadoc: List<String>?, val parameters: List<ParchmentParameterMapping>?)
    data class ParchmentFieldMapping(val name: String, val descriptor: String, val javadoc: List<String>?)
    data class ParchmentClassMapping(val name: String, val javadoc: List<String>?, val fields: List<ParchmentFieldMapping>?, val methods: List<ParchmentMethodMapping>?)
    data class ParchmentMappingJson(val version: String, val classes: List<ParchmentClassMapping>)  // TODO: support package comment

    private fun <T> readFileInZip(zipFilePath: Path, fileName: String, reader: Function<InputStream, T>): T? {
        FileInputStream(zipFilePath.toFile()).use { fileInputStream ->
            ZipInputStream(fileInputStream).use { zipInputStream ->
                var entry = zipInputStream.nextEntry
                while (entry != null) {
                    if (entry.name == fileName) {
                        return reader.apply(zipInputStream)
                    }
                    entry = zipInputStream.nextEntry
                }
            }
        }
        return null
    }

    override fun postProcessMergedVersion(version: VersionInfo, mappings: String?, target: MappingTarget, cache: Path, mappingTree: MappingTree?): CompletableFuture<MappingTree?> {
        if (mappingTree == null) return CompletableFuture.completedFuture(null)

        val parchmentZipPath = getPath(cache, version, mappings).resolve("parchment-${target.id}.zip")
        return requestText(maven.resolve("org/parchmentmc/data/parchment-${version.id}/maven-metadata.xml")).handle { it, e ->
            if (e != null) output(version.id, "requestText for parchment maven-metadata.xml failed: $e")
            if (e != null) null else it
        }.thenApply {
            if (it.isNullOrEmpty()) return@thenApply null
            val patternRelease = Pattern.compile("""^\s*<release>([\d.]+)</release>\s*$""")
            val patternLatest = Pattern.compile("""^\s*<latest>([\da-zA-Z.-]+)</latest>\s*$""")
            var latestVersion: String? = null
            it.lines().forEach { line ->
                val matcherRelease = patternRelease.matcher(line)
                if (matcherRelease.matches()) {
                    return@thenApply matcherRelease.group(1)
                }
                val matcherLatest = patternLatest.matcher(line)
                if (matcherLatest.matches()) {
                    latestVersion = matcherLatest.group(1)
                }
            }
            if (latestVersion == null) {
                output(version.id, "locate parchment mapping version for mc ${version.id} failed")
                output(version.id, it)
            }
            latestVersion
        }.thenCompose { parchmentVersion ->
            if (parchmentVersion == null) return@thenCompose CompletableFuture.completedFuture(null)
            helper.setMappingVersion(version, parchmentVersion)
            download(maven.resolve("org/parchmentmc/data/parchment-${version.id}/${parchmentVersion}/parchment-${version.id}-${parchmentVersion}.zip"), parchmentZipPath)
        }.thenApply {
            readFileInZip(parchmentZipPath, "parchment.json") { stream ->
                stream.bufferedReader().use {
                    Gson().fromJson(it, ParchmentMappingJson::class.java)
                }
            }
        }.thenApply {
            val parchmentMapping = it ?: return@thenApply null
            val resultMapping = applyParchmentMapping(mappingTree, parchmentMapping)

            helper.writeMetadata(cache, version, mappings)
            helper.writeComment(cache, version, mappings, resultMapping, resultMapping.namespaces[1])
            helper.registerComment(cache, version)

            resultMapping
        }
    }

    private fun applyParchmentMapping(mappingTree: MappingTree, parchmentMapping: ParchmentMappingJson): MappingTree {
        if (mappingTree.namespaces.size != 2) throw IllegalArgumentException("mojang mapping should have namespace size == 2. Actual namespaces: ${mappingTree.namespaces}")
        val inverted = mappingTree.invert(1)  // now deobfuscated names become the default names, so we can have index on them
        val parchmentClassMapping = parchmentMapping.classes.associateBy { it.name }
        inverted.classes.forEach { cm ->
            val pcm = parchmentClassMapping[cm.defaultName] ?: return@forEach
            cm.comment = pcm.javadoc?.joinToString("\n")

            pcm.fields?.forEach forEach2@ { pfm ->
                val fm = cm.fields[MemberDescriptor(pfm.name, pfm.descriptor)] ?: return@forEach2
                fm.comment = pfm.javadoc?.joinToString("\n")
            }
            pcm.methods?.forEach forEach2@ { pmm ->
                val mm = cm.methods[MemberDescriptor(pmm.name, pmm.descriptor)] ?: return@forEach2
                mm.comment = pmm.javadoc?.joinToString("\n")
                if (mm.parameters.isNotEmpty()) throw IllegalArgumentException("mojang mapping should not contains parameter mappings, found ${mm.parameters}")
                pmm.parameters?.forEach { ppm ->
                    mm.parameters[ppm.index] = ParameterImpl(ppm.name, ppm.javadoc)
                }
            }
        }
        return inverted.invert(1)
    }

    override fun canSkipFinishedMappedJar(cache: Path, version: VersionInfo): Boolean {
        return helper.checkSkipAndLoadIfOk(cache, version, null)
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
            }.thenCompose { merged -> provider.postProcessMerged(version, MappingTarget.MERGED, cache, merged) }
        }
    }
}