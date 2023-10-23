package text.indexer.demo

import text.indexer.demo.lib.IndexerServiceFactory

class IndexerServiceDemoApp{

}

fun main() {
    val indexerService = IndexerServiceFactory.createIndexertServiceWithDefaultTokenizer()
    indexerService.index("test")
    indexerService.search("someword")
}