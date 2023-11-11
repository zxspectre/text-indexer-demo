package text.indexer.demo.lib.impl

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import text.indexer.demo.lib.impl.fswatcher.EventType
import text.indexer.demo.lib.impl.fswatcher.FsWatcher
import text.indexer.demo.lib.impl.parser.DocumentProcessor
import text.indexer.demo.lib.impl.storage.MultimapBasedStorage
import text.indexer.demo.lib.impl.storage.ReverseIndexStorage
import text.indexer.demo.lib.impl.util.mbSizeString
import java.io.Closeable
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.isDirectory
import kotlin.io.path.pathString

private val log: Logger = LoggerFactory.getLogger(IndexerService::class.java)

private const val READ_WRITE_HEARTBEAT = 250L
private const val FILE_MEMORY_PRINT_FACTOR = 1.2
//^we could shift this memory factor based on previous indexation results in a certain range (e.g. [0.1 .. 1.2])
// as indexed file memory usage depends on the types of text and tokenizers used, which we do not know beforehand.
// For now as it's an edge case of an edge case, it's not a priority, just use safest value.

class IndexerService(
    customDelimiter: String?,
    tokenizer: ((String) -> Sequence<String>)?,
    indexerThreadPoolSize: Int = 2,
    private val tryToPreventOom: Boolean = true,
    private val maxWordLength: Int = 16384
) : Closeable {
    private val indexerServiceCoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val documentProcessor = DocumentProcessor(
        indexerServiceCoroutineScope.coroutineContext,
        tokenizer,
        customDelimiter
    ) { word, doc -> processWordCallback(word, doc) }

    private val customDispatcher = Executors.newFixedThreadPool(indexerThreadPoolSize).asCoroutineDispatcher()
    private val indexFilesJob = Job()
    private val processFileCoroutineScope = CoroutineScope(customDispatcher + indexFilesJob)

    private val watchService: FsWatcher = FsWatcher()
    private val reverseIndexStorage: ReverseIndexStorage<String, Path> = MultimapBasedStorage()

    private val currentlyIndexingFileSize: AtomicLong = AtomicLong(0)
    private val removalInProgress: AtomicBoolean = AtomicBoolean(false)


    init {
        indexerServiceCoroutineScope.launch {
            while (indexerServiceCoroutineScope.isActive) {
                val event = watchService.fileEventChannel.receive()
                when (event.type) {
                    EventType.NEW -> processNewFiles(event.files)
                    EventType.DELETED -> processDeletedFiles(event.files)
                }
            }
        }
    }


    private fun processNewFiles(files: Collection<Path>) {
        if (removalInProgress.get()) {
            throw RuntimeException("Shouldn't happen, ProgrammerNotFoundException.") //TODO remove removalInProgress after testing
        }
        files.forEach {
            processFileCoroutineScope.launch {
                index(it)
            }
        }
    }

    private suspend fun processDeletedFiles(files: Set<Path>) {
        try {
            removalInProgress.set(true)
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
        } finally {
            removalInProgress.set(false)
        }
    }


    suspend fun index(path: String) {
        watchService.watch(Path(path))
    }

    suspend fun unindex(path: String) {
        watchService.unwatch(Path(path))
    }

    fun getIndexedWordsCnt(): Int {
        return reverseIndexStorage.size()
    }

    fun getFilesSizeInIndexQueue(): Long {
        return currentlyIndexingFileSize.get()
    }

    private suspend fun index(file: Path) {
        require(!file.isDirectory()) { "Should specify file, but was directory: $file" }
        //TODO ?concurrent modification of indexed file - perhaps check TS before indexing and after
        val fileSize = file.fileSize()
        if (tryToPreventOom && oomPossible(fileSize)) {
            log.debug(" !!! Skip indexing file ${file.pathString} with size ${fileSize.mbSizeString()} as it may lead to OOM")
            return
        }
        log.debug("Indexing Start ${file.pathString}  --->")
        try {
            documentProcessor.extractWords(file)
            log.debug("  ---> Indexing Done ${file.pathString}")
        } finally {
            if (tryToPreventOom) {
                currentlyIndexingFileSize.addAndGet(-fileSize)
            }
        }
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
            } else {
                reverseIndexStorage.put(word.lowercase(), filePath) //TODO remove lowercase()
            }
        }
    }

    fun search(word: String): Collection<String> {
        log.trace("Searching for '$word'")
        val res = reverseIndexStorage.get(word.lowercase())   //TODO remove lowercase()
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