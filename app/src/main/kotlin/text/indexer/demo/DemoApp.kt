package text.indexer.demo

import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import text.indexer.demo.lib.IndexerServiceFactory

//class IndexerServiceDemoApp{}
private val log: Logger = LoggerFactory.getLogger("DemoApp")

suspend fun main() {
    val maxHeapMb = Runtime.getRuntime().maxMemory() / (1024 * 1024)
    log.info("Running with maxheap = $maxHeapMb MB")

    //TODO change lambdas from: List<String> to Sequence<String>
    //TODO add word postprocessor? implement case-insensitive index/search
    //TODO replace factory with builder? or have two implementations, separate one for the 4th case
    //TODO if file not a text - skip
    //TODO working with several instances of services
    //TODO working multithreaded with same service instance
    //TODO make service closeable, removing watches, freeing resources
//    val indexerService = IndexerServiceFactory.lambdaTokenizerIndexerService { s: String -> s.split(" ", ",", "\n") }
//    val indexerService = IndexerServiceFactory.wordExtractingIndexerService()
    val indexerService = IndexerServiceFactory.delimiterBasedIndexerService("""[\p{Punct}\s]+""")
//    indexerService.indexFile("app/src/main/resources/testfile.txt")
    indexerService.indexDirRecursive("app/src/main/resources")
    while (true){
        delay(5000)
        indexerService.search("is")
        log.debug("Using ${(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/ (1024 * 1024)}MB, indexed ${indexerService.getIndexedWordsCnt()} words.")
    }
}