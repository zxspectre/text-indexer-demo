package text.indexer.demo.lib.impl

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import text.indexer.demo.lib.impl.fswatcher.EventType
import text.indexer.demo.lib.impl.fswatcher.FSEvent
import text.indexer.demo.lib.impl.fswatcher.FsWatcher
import text.indexer.demo.lib.impl.parser.DocumentProcessor
import text.indexer.demo.lib.impl.storage.MapBasedStorage
import text.indexer.demo.lib.impl.storage.ReverseIndexStorage
import text.indexer.demo.lib.impl.util.mbSizeString
import java.io.Closeable
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.isDirectory
import kotlin.io.path.notExists
import kotlin.io.path.pathString

private val log: Logger = LoggerFactory.getLogger(IndexerService::class.java)

private const val READ_WRITE_HEARTBEAT = 250L
private const val FILE_MEMORY_PRINT_FACTOR = 1.5
//^we could shift this memory factor based on previous indexation results in a certain range (e.g. [0.1 .. 1.2])
// as indexed file memory usage depends on the types of text and tokenizers used, which we do not know beforehand.
// For now as it's an edge case of an edge case, it's not a priority, just use safest value.
/**
 * Indexer service.
 * See [text.indexer.demo.lib.IndexerServiceFactory].
 *
 * tryToPreventOom - will skip indexation if file size is too big for currently free memory and no files are indexed ATM
 *
 * maxWordLength - defines max length of word that will be indexed
 *
 * Also note this service's channel defined in [indexerErrorsChannel], that can be used for async notification about
 * problems.
 */
class IndexerService internal constructor(
    tokenizer: ((String) -> Sequence<String>)?,
    indexerThreadPoolSize: Int = 2,
    private val tryToPreventOom: Boolean = true,
    private val maxWordLength: Int = 16384
) : Closeable {
    /**
     * Buffered channel to which this service tries sending messages about indexation errors.
     * See [IndexError]
     */
    val indexerErrorsChannel = Channel<IndexError>(Channel.BUFFERED)
    val lastFileIndexedChannel = Channel<String>(Channel.BUFFERED)
    private val loggingExceptionHandler = CoroutineExceptionHandler { _, e ->
        log.error("Exception in IndexerService processes", e) // propagate this and childs exc into indexerErrorsChannel?
    }
    private val indexerServiceCoroutineScope = CoroutineScope(loggingExceptionHandler+SupervisorJob() + Dispatchers.Default)
    private val documentProcessor = DocumentProcessor(
        indexerServiceCoroutineScope.coroutineContext,
        tokenizer,
    ) { word, doc -> processWordCallback(word, doc) }

    private val customDispatcher = Executors.newFixedThreadPool(indexerThreadPoolSize).asCoroutineDispatcher()
    private val indexFilesJob = Job()
    private val processFileCoroutineScope = CoroutineScope(customDispatcher + indexFilesJob)

    private val watchService: FsWatcher = FsWatcher()
    private val reverseIndexStorage: ReverseIndexStorage<String, Path> = MapBasedStorage()
    private val filesWithBadWords = ConcurrentHashMap<Path, Long>()

    private val currentlyIndexingFileSize: AtomicLong = AtomicLong(0)
    private val currentlyIndexingFiles = ConcurrentHashMap<Path, Boolean>()
    private val lazilyInitialized = AtomicBoolean(false)


    suspend fun index(path: String) {
        index(Path(path))
    }

    suspend fun unindex(path: String) {
        unindex(Path(path))
    }

    suspend fun index(path: Path) {
        lazyInit()
        if (path.notExists()) {
            log.error("Specified path `$path` does not exist, cannot index")
            throw IllegalArgumentException("Specified path `$path` does not exist, cannot index")
        }
        watchService.watch(path)
    }

    private fun lazyInit() {
        if(!lazilyInitialized.get()){
            synchronized(lazilyInitialized){
                if(!lazilyInitialized.get()){
                    log.debug("lazy init")
                    indexerServiceCoroutineScope.launch {
                        while (indexerServiceCoroutineScope.isActive) {
                            val event = watchService.fileEventChannel.receive()
                            log.trace("After FS Poll: received event with type: ${event.type}")
                            when (event.type) {
                                EventType.NEW -> processNewFiles(event.files)
                                EventType.DELETED -> processDeletedFiles(event.files)
                            }
                        }
                    }
                    watchService.init()
                    lazilyInitialized.set(true)
                }
            }
        }
    }

    suspend fun unindex(path: Path) {
        watchService.unwatch(path)
    }

    fun getIndexedWordsCnt(): Int {
        return reverseIndexStorage.size()
    }

    fun getFilesSizeInIndexQueue(): Long {
        return currentlyIndexingFileSize.get()
    }

    fun getInprogressFiles(): Int {
        return currentlyIndexingFiles.size
    }


    private fun processNewFiles(files: Collection<Path>) {
        files.forEach {
            processFileCoroutineScope.launch {
                doIndex(it)
            }
        }
    }

    private suspend fun processDeletedFiles(files: Set<Path>) {
        while (indexFilesJob.children.filter { !it.isCompleted }.count() > 0) {
            //ideally wait on indexFilesJob.join() for only current children to complete instead
            delay(READ_WRITE_HEARTBEAT)
        }
        log.debug("Mark $files for deletion")
        reverseIndexStorage.remove(files)
        //we could cache several deletions and bulk-modify the inverted index,
        // but that also introduces additional concurrency between this cache and new additions to index
        // this may be easier to solve using some sort of optimistic locking (when we store timestamps in the index
        // and only apply if the change
    }


    private suspend fun doIndex(file: Path) {
        try {
            require(!file.isDirectory()) { "Should specify file, but was directory: $file" }
            val fileSize = file.fileSize()
            if (tryToPreventOom && oomPossible(fileSize)) {
                if (currentlyIndexingFiles.isNotEmpty()) {
                    log.debug(" ! Postpone indexing file ${file.pathString} with size ${fileSize.mbSizeString()} as it may lead to OOM")
                    indexerServiceCoroutineScope.launch {
                        delay(READ_WRITE_HEARTBEAT * 2)
                        watchService.fileEventChannel.send(FSEvent(EventType.NEW, setOf(file)))
                    }
                } else {
                    log.error(" !!! Skip indexing file ${file.pathString} with size ${fileSize.mbSizeString()} as it may lead to OOM")
                    indexerErrorsChannel.trySend(IndexError(ErrorType.FILE_TOO_BIG, fileSize.toString(), file))
                }
                return
            }
            if (currentlyIndexingFiles.contains(file)) {
                log.debug("Skip indexing $file as it is already indexing")
                return
            }
            updateMetaForIndexStart(file)
            try {
                documentProcessor.extractWords(file)
            } finally {
                updateMetaForIndexDone(file)
                if (tryToPreventOom) {
                    currentlyIndexingFileSize.addAndGet(-fileSize)
                }
                if(getInprogressFiles() == 0) lastFileIndexedChannel.trySend("") // just for CLI, refactor
            }
        } catch (ex: java.nio.charset.CharacterCodingException) {
            log.warn("Can index only UTF8 files, encountered another encoding/binary: $file")
            indexerErrorsChannel.trySend(IndexError(ErrorType.NON_UTF8_FILE, null, file))
        } catch (ex: Exception){
            log.error("Unexpected Exception [$ex] while indexing: $file")
            indexerErrorsChannel.trySend(IndexError(ErrorType.UNKNOWN_ERROR, ex.toString(), file))
        }
    }

    private fun updateMetaForIndexStart(file: Path) {
        log.debug("Indexing Start ${file.pathString}  --->")
        currentlyIndexingFiles[file] = true
        filesWithBadWords[file] = 0
    }


    private fun updateMetaForIndexDone(file: Path) {
        log.debug("  ---> Indexing Done ${file.pathString}")
        if (filesWithBadWords[file] == 0L) {
            filesWithBadWords.remove(file)
        } else {
            indexerErrorsChannel.trySend(IndexError(ErrorType.WORDS_SKIPPED, filesWithBadWords[file].toString(), file))
        }
        currentlyIndexingFiles.remove(file)
    }


    private fun oomPossible(fileSize: Long): Boolean {
        val allIndexingFilesSize = currentlyIndexingFileSize.get()
        val freeMemory =
            Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()
        if (allIndexingFilesSize + fileSize > (freeMemory / FILE_MEMORY_PRINT_FACTOR)) {
            Runtime.getRuntime().gc()
            if (allIndexingFilesSize + fileSize > (freeMemory / FILE_MEMORY_PRINT_FACTOR)) {
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

    private fun processWordCallback(word: String, filePath: Path) {
        if (word.isNotEmpty()) {
            if (word.length > maxWordLength) {
                log.info(
                    "Ignoring word <${word.substring(0, 10)}...> with length ${word.length} in file $filePath, " +
                            "current limit is $maxWordLength"
                )
                filesWithBadWords[filePath] = filesWithBadWords[filePath]!! + 1
            } else {
                reverseIndexStorage.put(word, filePath)
            }
        }
    }

    fun search(word: String): Collection<String> {
        log.trace("Searching for '$word'")
        val res = reverseIndexStorage.get(word)
            .map { it.pathString }
        log.debug("Found '$word' in $res")
        return res
    }

    override fun close() {
        watchService.close()
        customDispatcher.close()
        processFileCoroutineScope.cancel()
        indexerServiceCoroutineScope.cancel()
    }
}