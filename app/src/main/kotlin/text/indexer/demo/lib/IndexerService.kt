package text.indexer.demo.lib

interface IndexerService {
    fun index(path: String)

    fun search(word: String)
}