package text.indexer.demo.cli

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import text.indexer.demo.lib.impl.ErrorType
import text.indexer.demo.lib.impl.IndexerService
import text.indexer.demo.lib.impl.util.mbSizeString
import java.util.Date
import java.util.concurrent.ConcurrentHashMap

class IndexerCliApplication {
    private val commandSplitRegex = Regex(" ")
    private val maxWordLength = 16384
    private val indexerService =
        IndexerService(null, tryToPreventOom = true, maxWordLength = maxWordLength, indexerThreadPoolSize = 10)
    private val cliCoroutineScope = CoroutineScope(Dispatchers.Default)
    private val errors = ConcurrentHashMap<ErrorType, Int>()

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
                errors.merge(indexError.type, 1, Int::plus)
            }
        }

        cliCoroutineScope.launch {
            //handle indexation state
            while (cliCoroutineScope.isActive) {
                indexerService.lastFileIndexedChannel.receive()
                val errorStr = errors.entries.joinToString(", ") { (key, value) -> "$key: $value" }
                println("${date()} No more pending indexations. Indexed ${indexerService.getIndexedWordsCnt()} words. Encountered errors: $errorStr")
            }
        }

        while (cliCoroutineScope.isActive) {
            delay(1000)
        }
    }

    private suspend fun index(argument: List<String>) {
        val path = getArgument(argument)
        if (path.isNotEmpty()) {
            println("${date()} Adding path to indexation: $path")
            indexerService.index(path)
        }
    }

    private suspend fun remove(argument: List<String>) {
        val path = getArgument(argument)
        if (path.isNotEmpty()) {
            println("${date()} Removing path from indexation: $path")
            indexerService.unindex(path)
        }
    }

    private fun search(argument: List<String>) {
        val word = getArgument(argument)
        if (word.isNotEmpty()) {
            println("${date()} Documents containing [$word]:")
            val documents = indexerService.search(word).joinToString(separator = "\n")
            println(documents)
        }
    }

    private fun getArgument(split: List<String>): String {
        if (split.size < 2) {
            println("${date()} Incorrect command, should have argument: $split")
            return ""
        }
        return split[1]
    }

    private fun date(): String {
        return Date().toString()
    }

    private suspend fun processCommand(inputStr: String) {
        println()
        val split = inputStr.split(commandSplitRegex, 2)

        val command = split[0]
        try {
            when (command) {
                "index" -> index(split)
                "remove" -> remove(split)
                "search" -> search(split)
                "status" -> printStatus()
                "exit" -> exitCli()
                else -> {
                    println("${date()} Unknown command: $command, use: [index, remove, search, status, exit]")
                }
            }
        } catch (e: Exception) {
            println(e.message)
        }
    }

    private fun exitCli() {
        cliCoroutineScope.cancel()
        indexerService.close()
    }

    private fun printStatus() {
        Runtime.getRuntime().gc()
        println(
            "${date()} Using ${(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).mbSizeString()}, " +
                    "indexed ${indexerService.getIndexedWordsCnt()} words, " +
                    "inprogress=${indexerService.getInprogressFiles()}/${
                        indexerService.getFilesSizeInIndexQueue().mbSizeString()
                    }"
        )
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
        println(" ``` status ``` to output current indexation status")
        println(" ``` exit ``` to exit the app")
    }

    /**
     * Thu Nov 16 23:19:01 CET 2023 Adding path to indexation: /Users/disproper/test/intellij-community
     * Thu Nov 16 23:20:08 CET 2023 No more pending indexations. Indexed 1173391 words. Encountered errors: NON_UTF8_FILE: 1672, WORDS_SKIPPED: 1
     * Thu Nov 16 23:21:01 CET 2023 Using 1030MB, indexed 1173391 words, inprogress=0/0MB
     */
}