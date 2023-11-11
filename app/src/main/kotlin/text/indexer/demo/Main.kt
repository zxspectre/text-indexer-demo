package text.indexer.demo

import kotlinx.coroutines.delay
import text.indexer.demo.lib.IndexerServiceFactory

class Main {}

suspend fun main() {
    val regex = Regex("[\\p{Punct}\\s]++")
    IndexerServiceFactory.lambdaTokenizerIndexerService { s: String -> s.splitToSequence(regex) }.use {
        it.index("app/src/main/resources")
        delay(500)
        it.search("configuration")
    }
}