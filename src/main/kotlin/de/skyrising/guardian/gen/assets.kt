package de.skyrising.guardian.gen

import com.google.gson.Gson
import java.net.URI
import java.nio.file.Files
import java.util.concurrent.CompletableFuture

// see: $RUN_DIR/.cache/mc-versions/src/types.d.ts
private data class TempVersionManifestJson(val assetHash: String, val assetIndex: String, val lastModified: String)
private data class VersionInfoJson(val manifests: List<TempVersionManifestJson>)
private data class AssetIndexItem(val hash: String, val size: Int)
private data class AssetIndexJson(val objects: Map<String, AssetIndexItem>)

data class DownloadAssetsResultItem(val assetPath: String, val downloadPath: String)
data class DownloadAssetsResult(val indexUrl: String, val assets: List<DownloadAssetsResultItem>)

fun downloadAssets(version: VersionInfo, unit: ProgressUnit): CompletableFuture<DownloadAssetsResult?> =
    Timer(version.id, "downloadLangFiles").use {
        // $RUN_DIR/.cache/mc-versions/data/version/1.20.2.json
        val versionDetailsFilePath = MC_VERSIONS_DATA_DIR.resolve("version").resolve("$version.json")
        val versionDetails = Gson().fromJson(Files.newBufferedReader(versionDetailsFilePath), VersionInfoJson::class.java)

        // Take the last one, which should have the smallest lastModified
        // That's the manifest we want -- The first published manifest after the version got released
        // Reason: Mojang now keeps updating translation files for all MC versions >= 22w42a, and we don't want the latest translation file
        // See also: https://minecraft.wiki/w/Language?oldid=3085719#file-history:~:text=1.19.3-,22w42a,-Language%20files%20are
        val lastManifest = versionDetails.manifests.lastOrNull()
        val bestManifest = versionDetails.manifests.minByOrNull { it.lastModified }
        if (bestManifest == null) {
            output("assets", "no manifest for version ${version.id}, skipped")
            return CompletableFuture.completedFuture(null)
        }
        if (lastManifest != bestManifest) {
            output("assets", "version ${version.id} lastManifest (${lastManifest}) != bestManifest (${bestManifest})")
        }
        val assetIndexId = bestManifest.assetIndex
        val assetIndexSha1 = bestManifest.assetHash

        // https://piston-meta.mojang.com/v1/packages/6f3d9d0bdf359b83c5adc6b5482e1cd5c25ce1bb/8.json
        val assetIndexUrl = "https://piston-meta.mojang.com/v1/packages/${assetIndexSha1}/${assetIndexId}.json"
        if (assetIndexSha1.length < 2) {
            throw IllegalArgumentException("bad asset index sha1: $version $bestManifest")
        }

        val indexFilePath = ASSETS_DIR.resolve(assetIndexSha1.take(2)).resolve(assetIndexSha1)
        download(URI(assetIndexUrl), indexFilePath).thenCompose {
            if (!Files.exists(indexFilePath)) return@thenCompose null
            val assetIndex = GSON.fromJson(Files.newBufferedReader(indexFilePath), AssetIndexJson::class.java)

            data class WantedAsset(val path: String, val url: String, val hash: String)
            val wantedAssets = mutableListOf<WantedAsset>()
            assetIndex.objects.forEach { (path, item) ->
                if (item.hash.length < 2) {
                    return@forEach
                }
                if (path.startsWith("minecraft/lang/")) {
                    val assetUrl = "https://resources.download.minecraft.net/${item.hash.take(2)}/${item.hash}"
                    wantedAssets.add(WantedAsset(path, assetUrl, item.hash))
                }
            }
            output(version.id, "Assets download start (size=${wantedAssets.size})")

            val assetUnit = unit.subUnit(wantedAssets.size)
            val futures = mutableListOf<CompletableFuture<DownloadAssetsResultItem>>()
            for (wa in wantedAssets) {
                val assetPath = ASSETS_DIR.resolve(wa.hash.take(2)).resolve(wa.hash)
                futures.add(download(URI(wa.url), assetPath).thenApply {
                    assetUnit.done++
                    DownloadAssetsResultItem(
                        wa.path,
                        assetPath.toString()
                    )
                })
            }
            CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
                assetUnit.done = assetUnit.tasks
                futures.map { it.join() }
            }
        }.thenApply { items ->
            if (items == null) return@thenApply null
            output(version.id, "Assets download completed")
            DownloadAssetsResult(assetIndexUrl, items)
        }
    }
