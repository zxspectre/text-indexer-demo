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

//    delay(10000)
    val regex = Regex("[\\p{Punct}\\s]++")
//    val indexerService = IndexerServiceFactory.lambdaTokenizerIndexerService { s: String -> s.splitToSequence(regex)}
    val indexerService = IndexerServiceFactory.wordExtractingIndexerService()
//    val indexerService = IndexerServiceFactory.delimiterBasedIndexerService("""[\p{Punct}\s]+""")
//    val indexerService = IndexerService(customDelimiter = "===", tokenizer = {s: String -> s.splitToSequence(regex)})
//    indexerService.index("app/src/main/resources/fileof_randomness.txt")
    indexerService.index("app/src/main/resources")
//    repeat (3){
    while (true){
        delay(3000)
        indexerService.search("Sherlock")
        Runtime.getRuntime().gc()
        log.debug("Using ${(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).mbSizeString()}, " +
                "indexed ${indexerService.getIndexedWordsCnt()} words, " +
                "inprogress=${indexerService.getInprogressFiles()}/${indexerService.getFilesSizeInIndexQueue().mbSizeString()}")

    }
    indexerService.close()
    println(" ------  DONE   -------- ")
}