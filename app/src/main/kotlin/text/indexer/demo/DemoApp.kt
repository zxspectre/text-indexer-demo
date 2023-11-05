package text.indexer.demo

import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import text.indexer.demo.lib.IndexerServiceFactory
import text.indexer.demo.lib.impl.util.mbSizeString

//class IndexerServiceDemoApp{}
private val log: Logger = LoggerFactory.getLogger("DemoApp")

suspend fun main() {
    log.info("Running with maxheap = ${Runtime.getRuntime().maxMemory().mbSizeString()}")

    //TODO change lambdas from: List<String> to Sequence<String>
    //TODO add word postprocessor? implement case-insensitive index/search
    //TODO replace factory with builder? or have two implementations, separate one for the 4th case
    //TODO if file not a text - skip
    //TODO working with several instances of services
    //TODO working multithreaded with same service instance
    //TODO make service closeable, removing watches, freeing resources
    //TODO FileTooBigToIndexException breaks indexation (partial index?)
//    val indexerService = IndexerServiceFactory.lambdaTokenizerIndexerService { s: String -> s.split(" ", ",", "\n") }
//    val indexerService = IndexerServiceFactory.wordExtractingIndexerService()
    val indexerService = IndexerServiceFactory.delimiterBasedIndexerService("""[\p{Punct}\s]+""")
//    indexerService.index("app/src/main/resources/fileof_randomness.txt")
    indexerService.index("app/src/main/resources")
    while (true){
        delay(3000)
        indexerService.search("is")
        Runtime.getRuntime().gc()
        log.debug("Using ${(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).mbSizeString()}, " +
                "indexed ${indexerService.getIndexedWordsCnt()} words, inprogress=${indexerService.getFilesSizeInIndexQueue().mbSizeString()}")
    }
}