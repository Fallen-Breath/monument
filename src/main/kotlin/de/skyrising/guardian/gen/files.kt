package de.skyrising.guardian.gen

import com.google.gson.GsonBuilder
import com.google.gson.JsonElement
import java.io.BufferedInputStream
import java.io.File
import java.io.IOException
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URI
import java.nio.charset.Charset
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.ZipFile

data class DownloadProgress(val length: Long, val progress: Long)

private val DOWNLOADS = ConcurrentHashMap<Pair<URI, Path>, CompletableFuture<Unit>>()

fun download(url: URI, file: Path, listener: ((DownloadProgress) -> Unit)? = null) = DOWNLOADS.computeIfAbsent(Pair(url, file)) {
    startDownload(url, file, listener)
}

private const val MAX_REQUEST_ATTEMPTS = 5

fun <T> requestUrl(url: URI, func: ((HttpURLConnection) -> T)): T {
    var lastException = IOException()
    for (attempt in 1..MAX_REQUEST_ATTEMPTS) {
        val conn = url.toURL().openConnection() as HttpURLConnection
        conn.connect()
        try {
            return func(conn)
        } catch (e: IOException) {
            output("request", "Failed to request $url, attempt $attempt: $e")
            lastException = e
            Thread.sleep(500)
        }
    }
    throw IOException("Failed to download $url after $MAX_REQUEST_ATTEMPTS attempts", lastException)
}

private fun startDownload(url: URI, file: Path, listener: ((DownloadProgress) -> Unit)? = null) =
    supplyAsync(TaskType.DOWNLOAD) {
        Timer("", file.toString(), mapOf("url" to url.toString(), "file" to file.toString())).use {
            if (Files.exists(file) && !(file.fileName.toString().endsWith(".jar") && !isJarGood(file))) return@supplyAsync
            println("Downloading $url")
            Files.createDirectories(file.parent)
            if (url.scheme == "file") {
                Files.copy(Paths.get(url), file)
                return@supplyAsync
            }

            val tmpFile = file.resolveSibling(file.fileName.toString() + ".tmp")

            requestUrl(url) { conn ->
                val len = conn.getHeaderFieldLong("Content-Length", -1)
                BufferedInputStream(conn.inputStream).use { input ->
                    Files.newOutputStream(tmpFile).use { output ->
                        val buf = ByteArray(4096)
                        var progress = 0L
                        while (true) {
                            val read = input.read(buf)
                            if (read == -1) break
                            output.write(buf, 0, read)
                            progress += read
                            if (listener != null) listener(DownloadProgress(len, progress))
                        }
                    }
                }
            }

            Files.move(tmpFile, file, StandardCopyOption.REPLACE_EXISTING)
            Unit
        }
    }

inline fun <reified T : JsonElement> requestJson(url: URI): CompletableFuture<T> = supplyAsync(TaskType.DOWNLOAD) {
    println("Fetching $url")
    requestUrl(url) { conn ->
        GSON.fromJson<T>(InputStreamReader(conn.inputStream))
    }
}

fun requestText(url: URI, charset: Charset = Charsets.UTF_8): CompletableFuture<String> = supplyAsync(TaskType.DOWNLOAD) {
    println("Fetching $url")
    requestUrl(url) { conn ->
        conn.inputStream.bufferedReader(charset).use { reader -> reader.readText() }
    }
}

fun rmrf(path: Path) {
    Files.walkFileTree(path, object : SimpleFileVisitor<Path>() {
        override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
            Files.delete(file)
            return FileVisitResult.CONTINUE
        }

        override fun postVisitDirectory(dir: Path, exc: IOException?): FileVisitResult {
            if (exc != null) throw exc
            Files.delete(dir)
            return FileVisitResult.CONTINUE
        }
    })
}

fun copy(path: Path, to: Path, vararg options: CopyOption, filter: (Path) -> Boolean = { true }) {
    Files.walkFileTree(path, object : SimpleFileVisitor<Path>() {
        override fun preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult {
            val dest = to.resolve(path.relativize(dir).toString())
            Files.createDirectories(dest)
            return FileVisitResult.CONTINUE
        }

        override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
            val relative = path.relativize(file)
            if (filter(relative)) {
                val dest = to.resolve(relative.toString())
                Files.copy(file, dest, *options)
            }
            return FileVisitResult.CONTINUE
        }
    })
}

fun copyCached(path: Path, to: Path, cacheDir: Path, renameJarResource: Boolean = false) {
    Files.walkFileTree(path, object : SimpleFileVisitor<Path>() {
        override fun preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult {
            val dest = to.resolve(path.relativize(dir).toString())
            Files.createDirectories(dest)
            return FileVisitResult.CONTINUE
        }

        override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
            val content = Files.readAllBytes(file)
            var srcPath = file
            if (renameJarResource && file.fileName.toString().endsWith(".jar.resource")) {
                // We need to store those .jar resources files with a non-".jar" extension,
                // or the gradle shadow plugin will cause issues. Here's the operation to revert the rename
                // See: https://github.com/skyrising/monument/issues/1
                srcPath = file.resolveSibling(file.fileName.toString().removeSuffix(".jar.resource") + ".jar")
            }
            val dest = to.resolve(path.relativize(srcPath).toString())
            writeCached(dest, content, cacheDir)
            return FileVisitResult.CONTINUE
        }
    })
}

fun writeCached(path: Path, content: ByteArray, cacheDir: Path) {
    val hash = hex(sha256(content))
    val fileName = path.getName(path.nameCount - 1).toString()
    val extension = fileName.substringAfter('.', "")
    val suffix = if (extension.isNotEmpty()) ".$extension" else ""
    val cachePath = cacheDir.resolve(hash.take(2)).resolve(hash.substring(2) + suffix)
    if (!Files.exists(cachePath)) {
        Files.createDirectories(cachePath.parent)
        Files.write(cachePath, content)
    }
    Files.deleteIfExists(path)
    Files.createLink(path, cachePath)
}

private fun sha256(bytes: ByteArray) = MessageDigest.getInstance("SHA-256").digest(bytes)
private fun hex(bytes: ByteArray) =
    bytes.joinToString("") { ((it.toInt() shr 4) and 0xf).toString(16) + (it.toInt() and 0xf).toString(16) }

fun getJarFileSystem(jar: Path): FileSystem {
    val uri = jar.toUri()
    val fsUri = URI("jar:${uri.scheme}", uri.userInfo, uri.host, uri.port, uri.path, uri.query, uri.fragment)
    try {
        return FileSystems.newFileSystem(fsUri, mapOf<String, Any>())
    } catch (e: Exception) {
        throw RuntimeException("Error creating jar filesystem for $fsUri: $e", e)
    }
}

data class JarFileSystemsHolder(val fileSystems: List<FileSystem>) : AutoCloseable {
    override fun close() {
        fileSystems.forEach { it.close() }
    }
}

fun getJarFileSystems(jars: List<Path>): JarFileSystemsHolder = JarFileSystemsHolder(jars.map { getJarFileSystem(it) })

fun createJarFileSystem(jar: Path): FileSystem {
    Files.createDirectories(jar.parent)
    val uri = jar.toUri()
    val fsUri = URI("jar:${uri.scheme}", uri.userInfo, uri.host, uri.port, uri.path, uri.query, uri.fragment)
    return FileSystems.newFileSystem(fsUri, mapOf<String, Any>("create" to "true"))
}

interface PostProcessor {
    fun matches(path: Path): Boolean
    fun process(path: Path, content: ByteArray): Pair<Path, ByteArray>
}

val STRUCTURE_PROCESSOR = object : PostProcessor {
    private val structurePath = Paths.get("data", "minecraft", "structure")
    private val structuresPath = Paths.get("data", "minecraft", "structures")

    override fun matches(path: Path) =
        (path.startsWith(structurePath.toString()) || path.startsWith(structuresPath.toString())) && path.fileName.toString().endsWith(".nbt")

    override fun process(path: Path, content: ByteArray): Pair<Path, ByteArray> {
        val nbtName = path.fileName.toString()
        val snbtName = nbtName.dropLast(4) + ".snbt"
        val snbtOut = path.resolveSibling(snbtName)
        val tag = Tag.readCompressed(Files.newInputStream(path))
        convertStructure(tag)
        return Pair(snbtOut, tag.toSnbt().toByteArray())
    }
}

val SOURCE_PROCESSOR = object : PostProcessor {
    override fun matches(path: Path) = path.fileName.toString().endsWith(".java")

    override fun process(path: Path, content: ByteArray): Pair<Path, ByteArray> {
        val source = String(content).split("\n").toMutableList()
        var startingComment = source[0].startsWith("/*")
        var comment = false
        val it = source.listIterator()
        while (it.hasNext()) {
            val line = it.next()
            if (startingComment) {
                val index = line.indexOf("*/")
                when {
                    index < 0 -> it.remove()
                    index == line.length - 2 -> {
                        it.remove()
                        startingComment = false
                    }
                    else -> {
                        it.set(line.substring(index + 2))
                        startingComment = false
                    }
                }
                continue
            }
            if (line.contains("/*") && !line.contains("*/")) {
                comment = true
                continue
            }
            val lineComment = line.contains("//") && line.trim().startsWith("//")
            if ((comment || lineComment) && line.contains("at ")) {
                if (line.contains("de.skyrising.guardian.gen.")) {
                    it.remove()
                } else {
                    it.set(line.replace(Regex("\\(.*\\.(java|kt):\\d+\\)"), ""))
                }
            }
            if (line.contains("*/")) {
                comment = false
                continue
            }
        }
        return Pair(path, source.joinToString("\n", postfix="\n").toByteArray())
    }
}

val LANG_FILE_PROCESSOR = object : PostProcessor {
    private val langPath = Paths.get("assets", "minecraft", "lang")

    override fun matches(path: Path): Boolean = path.parent == langPath && path.fileName.toString().endsWith(".json")

    override fun process(path: Path, content: ByteArray): Pair<Path, ByteArray> {
        val gson = GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()
        val langMap = gson.fromJson(String(content, Charsets.UTF_8), LinkedHashMap<String, String>()::class.java)
            ?: throw RuntimeException("Could not parse $path as json")
        return Pair(path, gson.toJson(langMap).toByteArray(Charsets.UTF_8))
    }
}

fun postProcessFile(path: Path, relative: Path, postProcessors: List<PostProcessor>): Pair<Path, ByteArray?> {
    var outRelative = relative
    var content: ByteArray? = null
    val appliedPostProcessors = mutableSetOf<PostProcessor>()
    outer@ while (appliedPostProcessors.size < postProcessors.size) {
        for (processor in postProcessors) {
            if (processor in appliedPostProcessors) continue
            if (processor.matches(outRelative)) {
                if (content == null) content = Files.readAllBytes(path)
                try {
                    val result = processor.process(outRelative, content!!)
                    outRelative = result.first
                    content = result.second
                } catch (e: Exception) {
                    e.printStackTrace()
                }
                appliedPostProcessors.add(processor)
                continue@outer
            }
        }
        break
    }
    return Pair(outRelative, content)
}

fun postProcessSources(version: String, srcTmpDir: Path, srcDir: Path, postProcessors: List<PostProcessor>) =
    supplyAsync(TaskType.POST_PROCESS) {
        Timer(version, "postProcessSources", mapOf("srcDir" to srcDir.toString())).use {
            if (Files.exists(srcDir)) rmrf(srcDir)
            Files.createDirectories(srcDir)
            Files.walk(srcTmpDir).forEach { path ->
                if (Files.isDirectory(path)) return@forEach
                val relative = srcTmpDir.relativize(path)
                val (outRelative, content) = postProcessFile(path, relative, postProcessors)
                val fileOut = srcDir.resolve(outRelative.toString())
                Files.createDirectories(fileOut.parent)
                if (content != null) {
                    Files.write(fileOut, content)
                } else {
                    Files.copy(path, fileOut)
                }
            }
            if (srcTmpDir.fileSystem != FileSystems.getDefault()) {
                srcTmpDir.fileSystem.close()
            }
        }
    }

fun extractResources(version: String, jar: Path, out: Path, postProcessors: List<PostProcessor>) =
    supplyAsync(TaskType.EXTRACT_RESOURCE) {
        Timer(version, "extractResources", mapOf("jar" to jar.toString(), "out" to out.toString())).use {
            getJarFileSystem(jar).use { fs ->
                val root = fs.getPath("/")
                Files.walk(root).forEach { path ->
                    if (Files.isDirectory(path) || path.fileName.toString().endsWith(".class")) return@forEach
                    val relative = root.relativize(path)
                    val (outRelative, content) = postProcessFile(path, relative, postProcessors)
                    val fileOut = out.resolve(outRelative.toString())
                    Files.createDirectories(fileOut.parent)
                    if (content == null) {
                        copyCached(path, fileOut, RESOURCE_CACHE_DIR)
                    } else {
                        writeCached(fileOut, content, RESOURCE_CACHE_DIR)
                    }
                }
            }
        }
    }

fun copyAssets(assets: DownloadAssetsResult, resOut: Path, postProcessors: List<PostProcessor>) {
    for (asset in assets.assets) {
        val src = Path.of(asset.downloadPath)
        val (outRelative, content) = postProcessFile(src, Paths.get("assets", asset.assetPath), postProcessors)
        val dst = resOut.resolve(outRelative)
        Files.createDirectories(dst.parent)
        if (content == null) {
            copyCached(src, dst, RESOURCE_CACHE_DIR)
        } else {
            writeCached(dst, content, RESOURCE_CACHE_DIR)
        }
    }
}

fun convertStructure(tag: Tag) {
    if (tag !is CompoundTag) return
    tag.remove("DataVersion")
    val paletteTag = tag["palette"]
    val blocksTag = tag["blocks"]
    if (paletteTag !is ListTag<*> || blocksTag !is ListTag<*>) return
    val palette = mutableListOf<String>()
    tag["palette"] = ListTag(ArrayList(paletteTag.map { e ->
        if (e !is CompoundTag) throw IllegalArgumentException("palette entry should be a CompoundTag: $e")
        val str = tagToBlockStateString(e)
        palette.add(str)
        StringTag(str)
    }))
    tag["data"] = ListTag(ArrayList(blocksTag.map { block ->
        if (block !is CompoundTag) throw IllegalArgumentException("block should be a CompoundTag: $block")
        val stateId = block["state"].let { it as? IntTag } ?: return@map block
        block["state"] = StringTag(palette[stateId.value])
        block
    }))
    tag.remove("blocks")
    return
}

fun tagToBlockStateString(tag: CompoundTag): String {
    val name = tag["Name"].let { if (it is StringTag) it.value else "minecraft:air" }
    val props = tag["Properties"].let { it as? CompoundTag } ?: return name
    val sb = StringBuilder(name).append('{')
    var first = true
    for ((k, v) in props) {
        if (v !is StringTag) continue
        if (!first) sb.append(',')
        first = false
        sb.append(k).append(':').append(v.value)
    }
    return sb.append('}').toString()
}

fun useResourceFileSystem(cls: Class<*> = Dummy::class.java, fn: (Path) -> Unit) {
    val root = cls.getResource("/.resourceroot") ?: throw IllegalStateException("Could not find resource root")
    val uri = root.toURI()
    when (uri.scheme) {
        "file" -> fn(Paths.get(uri).parent)
        "jar" -> {
            try {
                // FIXME: this file system isn't closed because it has multiple users
                fn(FileSystems.newFileSystem(uri, emptyMap<String, Any>()).getPath("/"))
            } catch (e: FileSystemAlreadyExistsException) {
                fn(FileSystems.getFileSystem(uri).getPath("/"))
            }
        }
        else -> throw IllegalStateException("Cannot get file system for scheme '${uri.scheme}'")
    }
}

fun getMavenArtifact(mvnArtifact: MavenArtifact): CompletableFuture<URI> {
    val path = mvnArtifact.getPath()
    val filePath = JARS_DIR.resolve("libraries").resolve(path)
    if (Files.exists(filePath)) return CompletableFuture.completedFuture(filePath.toUri())
    val url = mvnArtifact.mavenUrl.resolve(path)
    return download(url, filePath).thenApply { filePath.toUri() }
}

fun getMavenArtifacts(mvnArtifacts: List<MavenArtifact>): CompletableFuture<List<URI>> {
    val futures = mvnArtifacts.map(::getMavenArtifact)
    return CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
        futures.map(CompletableFuture<URI>::get)
    }
}

private object Dummy

fun getMonumentClassRoot(): Path? {
    val dummyClass = Dummy::class.java
    val dummyFileName = File.separator + dummyClass.name.replace('.', File.separatorChar) + ".class"
    val dummyUrl = dummyClass.getResource(dummyFileName)
        ?: return null
    val uri = dummyUrl.toURI()
    return when (uri.scheme) {
        "file" -> {
            val p = Paths.get(uri).toString()
            Paths.get(p.substringBefore(dummyFileName))
        }

        "jar" -> Paths.get(uri.schemeSpecificPart.substring(5, uri.schemeSpecificPart.indexOf('!')))
        else -> null
    }
}

fun isJarGood(jarPath: Path, minSize: Int = 0): Boolean {
    return try {
        if (Files.size(jarPath) < minSize) {
            return false
        }
        ZipFile(jarPath.toFile()).use { zip ->
            for (entry in zip.entries()) {
                zip.getInputStream(entry).use { }
            }
        }
        true
    } catch (e: Exception) {
        output("jarVerifier", "bad jar at $jarPath")
        false
    }
}