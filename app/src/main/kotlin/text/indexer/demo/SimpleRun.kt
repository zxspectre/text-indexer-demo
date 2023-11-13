package text.indexer.demo

import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import text.indexer.demo.lib.IndexerServiceFactory
import text.indexer.demo.lib.impl.util.mbSizeString


private val log: Logger = LoggerFactory.getLogger("DemoApp")

class Main {}

suspend fun main() {
    log.info("Running with maxheap = ${Runtime.getRuntime().maxMemory().mbSizeString()}")
    val regex = Regex("[\\p{Punct}\\s]++")
    IndexerServiceFactory.lambdaTokenizerIndexerService { s: String -> s.splitToSequence(regex) }.use {
        it.index("app/src/main/resources")
        delay(500)
        it.search("configuration")

//        while (true) {
//            delay(3000)
//            it.search("Sherlock")
//            Runtime.getRuntime().gc()
//            log.debug(
//                "Using ${(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).mbSizeString()}, " +
//                        "indexed ${it.getIndexedWordsCnt()} words, " +
//                        "inprogress=${it.getInprogressFiles()}/${
//                            it.getFilesSizeInIndexQueue().mbSizeString()
//                        }"
//            )
//            val tryReceive = it.indexerErrorsChannel.tryReceive()
//            if(tryReceive.isSuccess){
//                System.err.println(tryReceive.getOrNull())
//            }
//        }
    }


}