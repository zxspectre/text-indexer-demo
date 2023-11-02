package text.indexer.demo.lib.impl

import com.google.common.collect.Multimaps
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
import text.indexer.demo.lib.IndexerService
import text.indexer.demo.lib.impl.exceptions.FileTooBigToIndexException
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Path
import java.util.Scanner
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.isDirectory
import kotlin.io.path.pathString

private const val MAX_WORD_LENGTH = 16384
private val log: Logger = LoggerFactory.getLogger(IndexerServiceImpl::class.java)


class IndexerServiceImpl(
    delimiter: String?,
    private val tokenizer: ((String) -> List<String>)?,
    private val indexerThreadPoolSize: Int = 2,
    private val tryToPreventOom: Boolean = true
) : IndexerService, Closeable {


    private val indexationCoroutineScope = CoroutineScope(getDispatcher())
    private var wordToFileMap = Multimaps.newSetMultimap(ConcurrentHashMap<String, Collection<Path>>()) {
        ConcurrentHashMap.newKeySet() //TODO make a 2nd impl with Trie, improve search traverse?
    }
    private var deletedIndexedFiles = HashSet<Path>() // TODO replace w' some append specific concurrent set
    private var externallyMarkedForDeletionFiles = HashSet<Path>()

    private val customDelimiter: String? = delimiter
    private val watchService: FSPollingWatcher = FSPollingWatcher()
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

        //TODO scope.launch {merge deletedIndexedFiles with externallyMarkedForDeletionFiles; update wordToFileMap with deletedIndexedFiles}
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
        deletedIndexedFiles.add(file)
    }

    override suspend fun index(path: String) {
        TODO("Not yet implemented")
    }

    override suspend fun unindex(path: String) {
        TODO("Not yet implemented")
        // if (is_file) externallyMarkedForDeletionFiles.add(it) else ???
    }

    override fun getIndexedWordsCnt(): Int {
        return wordToFileMap.size()
    }

    override fun getFilesSizeInIndexQueue(): Long {
        return currentlyIndexingFileSize.get()
    }

    override suspend fun indexDirRecursive(path: String) {
        val dir = Path(path)
        watchService.register(dir)

        //TODO remove this indexing, use watchService's signals
//        dir.walk().forEach {
//            if (it.isFile) {
//                index(it.toPath())
//            }
//        }
    }

    override suspend fun indexFile(filePath: String) {
        watchService.register(Path(filePath))
    }

    private suspend fun _indexFile(filePath: String) {
        val filePath = Path(filePath)
        watchService.register(filePath)
        //TODO check TS before indexing
        //TODO concurrent modification of indexed file - ?
//        index(filePath)
        //TODO check and store TS after indexing
    }

    private suspend fun index(file: Path) {
        require(!file.isDirectory()) { "Should specify file, but was directory: $file" }
        //TODO concurrent file modification while indexing?
        val fileSize = file.fileSize()
        if (tryToPreventOom) {
            tryPreventOom(file, fileSize)
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
            log.debug("Indexing Done ${file.pathString}")
        } finally {
            if (tryToPreventOom) {
                currentlyIndexingFileSize.addAndGet(-fileSize)
            }
        }
    }

    private fun tryPreventOom(file: Path, fileSize: Long) {
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
                throw FileTooBigToIndexException("Skip indexing file ${file.pathString} with size ${fileSize.mbSizeString()} as it may lead to OOM")
            }
        }
        currentlyIndexingFileSize.getAndAdd(fileSize)
    }

    private fun defaultTokenizer(line: String): List<String> {
        return line.split("\\s+".toRegex()).map { it.replace("""^\p{Punct}+|\p{Punct}+$""".toRegex(), "") }
    }

    private fun applyTokenizerAndProcessWords(tokenizer: ((String) -> List<String>)?, word: String, filePath: Path) {
        tokenizer!!.invoke(word).forEach { processWord(it, filePath) }
    }

    private fun processWord(word: String, filePath: Path) {
        if (word.isNotEmpty()) {
            if (word.length > MAX_WORD_LENGTH) {
                log.info(
                    "Ignoring word <${word.substring(0, 10)}...> with length ${word.length} in file $filePath, " +
                            "current limit is $MAX_WORD_LENGTH"
                )
            } else {
                wordToFileMap.put(word, filePath)
            }
        }
    }

    override fun search(word: String): Collection<String> {
        log.debug("Searching for '$word'")
        val result = wordToFileMap[word]
        val res = result
            .filter { !deletedIndexedFiles.contains(it) }
            .map { it.pathString }
        log.info("Found '$word' in $res")
        return res
    }

    override fun close() {
        watchService.close()
        indexationCoroutineScope.cancel()
    }
}