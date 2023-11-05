package text.indexer.demo.lib.impl

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import text.indexer.demo.lib.impl.fswatcher.FSPollingWatcher
import text.indexer.demo.lib.impl.storage.MapBasedStorage
import text.indexer.demo.lib.impl.storage.ReverseIndexStorage
import text.indexer.demo.lib.impl.util.mbSizeString
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Path
import java.util.Scanner
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.isDirectory
import kotlin.io.path.pathString

private val log: Logger = LoggerFactory.getLogger(IndexerService::class.java)

class IndexerService(
    private val customDelimiter: String?,
    private val tokenizer: ((String) -> List<String>)?,
    private val indexerThreadPoolSize: Int = 2,
    private val tryToPreventOom: Boolean = true,
    private val maxWordLength: Int = 16384 //TODO limit buffer length to this, when searching for delimiters (4binaries)
) : Closeable {

    private val indexationCoroutineScope = CoroutineScope(getDispatcher())

    private val watchService: FSPollingWatcher = FSPollingWatcher()
    private val reverseIndexStorage: ReverseIndexStorage<String, Path> = MapBasedStorage()

    private val currentlyIndexingFileSize: AtomicLong = AtomicLong(0)


    init {
        indexationCoroutineScope.launch {
            while (indexationCoroutineScope.isActive) {
                log.trace("check modified files")
                processFileModifiedEvent(watchService.fileModifiedChannel.receive())
            }
        }
        indexationCoroutineScope.launch {
            while (indexationCoroutineScope.isActive) {
                log.trace("check deleted files")
                processFileDeletedEvent(watchService.fileDeletedChannel.receive())
            }
        }
    }

    private fun getDispatcher(): ExecutorCoroutineDispatcher {
        return Executors.newFixedThreadPool(indexerThreadPoolSize).asCoroutineDispatcher()
    }

    private suspend fun processFileModifiedEvent(file: Path) {
        indexationCoroutineScope.launch {
            index(file)
        }
    }

    private suspend fun processFileDeletedEvent(file: Path) {
        log.debug("Mark $file for deletion")
        reverseIndexStorage.remove(file)
    }

    suspend fun index(path: String) {
        watchService.register(Path(path))
    }

    suspend fun unindex(path: String) {
        watchService.deregister(Path(path))
    }

    fun getIndexedWordsCnt(): Int {
        return reverseIndexStorage.size()
    }

    fun getFilesSizeInIndexQueue(): Long {
        return currentlyIndexingFileSize.get()
    }

    private suspend fun index(file: Path) {
        //TODO extract method implementation into separate DocumentProcessor classes
        //TODO extract method implementation into separate DocumentProcessor classes
        //TODO extract method implementation into separate DocumentProcessor classes
        require(!file.isDirectory()) { "Should specify file, but was directory: $file" }

        //TODO ?check TS before indexing
        //TODO ?concurrent modification of indexed file
        val fileSize = file.fileSize()
        if (tryToPreventOom && oomPossible(fileSize)){
            log.debug(" !!! Skip indexing file ${file.pathString} with size ${fileSize.mbSizeString()} as it may lead to OOM")
            return
        }
        log.debug("Indexing Start ${file.pathString}")

//        if (tokenizer == null && customDelimiter == null) {
//            println("Standard word extractor")
//        } else if (tokenizer == null && customDelimiter != null) {
//            println("Custom delimiter extractor")
//        } else if (tokenizer != null && customDelimiter == null) {
//            println("Tokenizer that iterates on lines")
//        } else if (tokenizer != null && customDelimiter != null) {
//            println("Tokenizer that iterates on custom tokens")
//        }

        try {
            withContext(Dispatchers.IO) {
                if (customDelimiter != null) {
                    Scanner(file).use {
                        it.useDelimiter(customDelimiter)
                        while (it.hasNext()) {
                            val delimitedText = it.next()
                            if (tokenizer != null) {
                                applyTokenizerAndProcessWords(tokenizer, delimitedText, file)
                            } else {
                                processWord(delimitedText, file)
                            }
                        }
                    }
                } else {
                    Files.newBufferedReader(file).use {
                        while (it.ready()) {
                            applyTokenizerAndProcessWords(
                                tokenizer ?: { s: String -> defaultTokenizer(s) },
                                it.readLine(),
                                file
                            )
                        }
                    }
                }
            }
            //TODO ?check and store TS after indexing
            log.debug("Indexing Done ${file.pathString}")
        } finally {
            if (tryToPreventOom) {
                currentlyIndexingFileSize.addAndGet(-fileSize)
            }
        }
    }

    private fun oomPossible(fileSize: Long): Boolean {
        val memoryPrintFactor = 1.2 //we could shift this memory factor based on previous indexation results in a certain range (e.g. [0.1 .. 1.2])
        // as indexed file memory usage depends on the types of text and tokenizers used, which we do not know beforehand.
        // For now as it's an edge case of an edge case, it's not a priority, just use safest value.
        val allIndexingFilesSize = currentlyIndexingFileSize.get()
        val freeMemory =
            Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()
        if (allIndexingFilesSize + fileSize > (freeMemory / memoryPrintFactor)) {
            Runtime.getRuntime().gc()
            if (allIndexingFilesSize + fileSize > (freeMemory / memoryPrintFactor)) {
                log.debug(
                    "Currently indexing ${allIndexingFilesSize.mbSizeString()}, with suggested file " +
                            "its ${(allIndexingFilesSize + fileSize).mbSizeString()} " +
                            "versus ${freeMemory.mbSizeString()} free heap after GC"
                )
                return true
            }
        }
        currentlyIndexingFileSize.getAndAdd(fileSize)
        return false
    }

    private fun defaultTokenizer(line: String): List<String> {
        return line.split("\\s+".toRegex()).map { it.replace("""^\p{Punct}+|\p{Punct}+$""".toRegex(), "") }
    }

    private fun applyTokenizerAndProcessWords(tokenizer: ((String) -> List<String>)?, word: String, filePath: Path) {
        tokenizer!!.invoke(word).forEach { processWord(it, filePath) }
    }

    private fun processWord(word: String, filePath: Path) {
        if (word.isNotEmpty()) {
            if (word.length > maxWordLength) {
                log.info(
                    "Ignoring word <${word.substring(0, 10)}...> with length ${word.length} in file $filePath, " +
                            "current limit is $maxWordLength"
                )
            } else {
                reverseIndexStorage.put(word.lowercase(), filePath)
            }
        }
    }

    fun search(word: String): Collection<String> {
        log.debug("Searching for '$word'")
        val res = reverseIndexStorage.get(word.lowercase())
            .map { it.pathString }
        log.info("Found '$word' in $res")
        return res
    }

    override fun close() {
        watchService.close()
        indexationCoroutineScope.cancel()
    }
}