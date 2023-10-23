package text.indexer.demo.lib.impl

import text.indexer.demo.lib.IndexerService

class IndexerServiceImpl : IndexerService {
    override fun index(path: String) {
        println(path)
    }

    override fun search(word: String) {
        println(word)
    }
}