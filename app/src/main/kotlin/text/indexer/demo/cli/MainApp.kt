package text.indexer.demo.cli

import kotlinx.coroutines.runBlocking

class MainApp {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val cli = IndexerCliApplication()
            runBlocking {
                cli.start()
            }
        }
    }
}