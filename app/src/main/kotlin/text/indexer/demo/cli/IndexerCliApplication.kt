package text.indexer.demo.cli

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import text.indexer.demo.lib.impl.IndexerService
import text.indexer.demo.lib.impl.util.mbSizeString
import java.util.concurrent.atomic.AtomicBoolean

class IndexerCliApplication {
    private val commandSplitRegex = Regex(" ")
    private val maxWordLength = 16384
    private val indexerService = IndexerService(null, tryToPreventOom = true, maxWordLength = maxWordLength)
    private val cliCoroutineScope = CoroutineScope(Dispatchers.Default)
    private val shouldNotify = AtomicBoolean(false)
    suspend fun start() {
        printHello()

        cliCoroutineScope.launch {
            //handle CLI
            while (cliCoroutineScope.isActive) {
                val command = readln()
                processCommand(command)
            }
        }

        cliCoroutineScope.launch {
            //handle indexation errors
            while (cliCoroutineScope.isActive) {
                val indexError = indexerService.indexerErrorsChannel.receive()
                println(" ERR: $indexError") //handle errors human-readably
            }
        }

        cliCoroutineScope.launch {
            //handle indexation state
            while (cliCoroutineScope.isActive) {
                delay(250)
                val inprogressFiles = indexerService.getInprogressFiles()
                delay(250)
                val inprogressFiles2 = indexerService.getInprogressFiles()

                if (inprogressFiles == 0 && inprogressFiles2 == 0 && shouldNotify.get()) {
                    shouldNotify.set(false)
                    println("No more pending indexations. Indexed ${indexerService.getIndexedWordsCnt()} words.")
                }
            }
        }

        while (true) {
            delay(1000)
        }
    }

    private suspend fun index(argument: String) {
        shouldNotify.set(true)
        indexerService.index(argument)
    }

    private suspend fun remove(argument: String) {
        shouldNotify.set(true)
        indexerService.unindex(argument)
    }

    private fun search(argument: String) {
        println(indexerService.search(argument).joinToString(separator = "\n"))
    }


    private suspend fun processCommand(inputStr: String) {
        println()
        val split = inputStr.split(commandSplitRegex, 2)
        if (split.size < 2) {
            println("Incorrect command, should have argument: $split")
            return
        }
        val command = split[0]
        val argument = split[1]
        try {
            when (command) {
                "index" -> index(argument)
                "remove" -> remove(argument)
                "search" -> search(argument)
                else -> {
                    println("Unknown command: $command")
                }
            }
        } catch (e: Exception) {
            println(e.message)
        }
    }

    private fun printHello() {
        println(" ================= ")
        println(
            "Running default word extracting indexer with OOM prevention and max word length = $maxWordLength. " +
                    "You can edit those in `text.indexer.demo.cli.IndexerCliApplication`"
        )
        println(
            "Running with maxheap = ${
                Runtime.getRuntime().maxMemory().mbSizeString()
            }. You can change it in [build.gradle], see `applicationDefaultJvmArgs`"
        )
        println("You can now enter commands separated by a new line or Ctrl-C to exit.")
        println("Valid commands are:")
        println(" ``` index <path> ``` to add <path> to the indexation, it may be file or dir, but it should exist")
        println(" ``` remove <path> ``` to remove <path> from indexation, it will be no longer watched and removed from index")
        println(" ``` search <word> ``` to search for documents with <word> in it, each document will be printed as stdout line")
    }


}