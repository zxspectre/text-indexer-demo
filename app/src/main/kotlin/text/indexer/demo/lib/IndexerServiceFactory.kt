package text.indexer.demo.lib

import text.indexer.demo.lib.impl.IndexerServiceImpl

class IndexerServiceFactory {

    companion object{
        fun createIndexertServiceWithDefaultTokenizer() : IndexerService{
            return IndexerServiceImpl()
        }
    }
}