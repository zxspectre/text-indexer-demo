package text.indexer.demo.lib

interface IndexerService {
    suspend fun indexFile(path: String)
    suspend fun indexDirRecursive(path: String)
    suspend fun index(path: String)
    suspend fun unindex(path: String)

    fun search(word: String): Collection<String>
}