package text.indexer.demo.lib.impl.fswatcher

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries


class FsWatcher(val pollingIntervalMillis: Long = 2000L) : Closeable {
    val fileEventChannel = Channel<FSEvent>(Channel.BUFFERED)

    private val shouldForcePoll = AtomicBoolean(false)
    private val watchRequestChannel = Channel<WatchRequest>(Channel.BUFFERED)

    private val log: Logger = LoggerFactory.getLogger(FsWatcher::class.java)

    private val watchedFilesLock = ReentrantLock()
    private val mutableWatchedFiles = HashSet<Path>()
    private val mutableWatchedDirs = HashSet<Path>()

    // no concurrent access
    private val fileModificationTimestamps = HashMap<Path, Long>()

    private val scope = CoroutineScope(Dispatchers.Default)


    suspend fun watch(path: Path) {
        require(path.exists()) { "Path does not exist" } //or have two methods or create separate 'waiting' list
        val reqType = when {
            path.isDirectory() -> RequestType.NEW_DIR
            else -> RequestType.NEW_FILE
        }
        watchRequestChannel.send(WatchRequest(reqType, path))
    }

    suspend fun unwatch(path: Path) {
        watchRequestChannel.send(WatchRequest(RequestType.DELETE, path))
    }


    init {
        processWatchRequestMessages()
        pollForFsChanges()
    }

    private fun processWatchRequestMessages() {
        scope.launch {
            while (scope.isActive) {
                updateWatchedFilesDirs(watchRequestChannel.receive())
            }
        }
    }

    private fun updateWatchedFilesDirs(watchMsg: WatchRequest) {
        log.trace("Received request to change polled entries, type: ${watchMsg.type}")
        watchedFilesLock.run {
            when (watchMsg.type) {
                RequestType.NEW_FILE -> mutableWatchedFiles.add(watchMsg.path)
                RequestType.NEW_DIR -> mutableWatchedDirs.add(watchMsg.path)
                RequestType.DELETE -> mutableWatchedFiles.remove(watchMsg.path) || mutableWatchedDirs.remove(watchMsg.path)
            }
        }
        shouldForcePoll.set(true)
    }


    private fun pollForFsChanges() {
        var previouslyPolledFiles = HashSet<Path>()
        scope.launch {
            delay(100) //small pause after creation if some files are immediately added
            var watchedFiles: Set<Path>
            var watchedDirs: Set<Path>
            while (scope.isActive) {
                watchedFilesLock.run {
                    watchedFiles = HashSet(mutableWatchedFiles)
                    watchedDirs = HashSet(mutableWatchedDirs)
                }
                log.trace("polling FS, ${watchedFiles.size} files / ${watchedDirs.size} dirs")
                // Handle changes to watched directories
                val currentDirFiles = HashSet<Path>()
                watchedDirs.forEach { collectFilesInDirRecurse(it, currentDirFiles) }
                val deletedFiles = previouslyPolledFiles.subtract(currentDirFiles).subtract(watchedFiles).toMutableSet()

                val currentFiles = HashSet<Path>(currentDirFiles)
                watchedFiles.forEach {
                    if (it.exists()) {
                        currentFiles.add(it)
                    } else {
                        deletedFiles.add(it)
                    }
                }

                deletedFiles.forEach {
                    log.trace("file deleted $it")
                    fileModificationTimestamps.remove(it)
                }
                //handle mayBeModifiedFiles
                val modifiedFiles = HashSet<Path>()
                val newFiles = HashSet<Path>()
                currentFiles.forEach {
                    val lastModified = Files.getLastModifiedTime(it).toMillis()
                    if (!fileModificationTimestamps.containsKey(it)) {
                        newFiles.add(it)
                        fileModificationTimestamps[it] = lastModified
                    } else if (fileModificationTimestamps[it] != lastModified) {
                        modifiedFiles.add(it)
                        fileModificationTimestamps[it] = lastModified
                    }
                }
                sendEvent(EventType.DELETED, modifiedFiles, deletedFiles)

                sendEvent(EventType.NEW, newFiles, modifiedFiles)

                previouslyPolledFiles = currentFiles

                val start = System.currentTimeMillis()
                while(System.currentTimeMillis() - start < pollingIntervalMillis) {
                    if(shouldForcePoll.get()){
                        shouldForcePoll.set(false)
                        break
                    }
                    delay(100)
                }
            }
            log.info("Terminating poller loop")
            fileEventChannel.close()
        }


    }

    private suspend fun sendEvent(type: EventType, vararg files: Collection<Path>) {
        val eventFiles = HashSet<Path>()
        for (fileCollection in files) {
            eventFiles.addAll(fileCollection)
        }
        if (eventFiles.isNotEmpty()) {
            log.debug("Send ${eventFiles.size} files with eventType=$type")
            fileEventChannel.send(FSEvent(type, eventFiles))
        }
    }

    private fun collectFilesInDirRecurse(directory: Path, walkedFiles: MutableSet<Path>) {
        val files = directory.listDirectoryEntries()
        for (file in files) {
            if (file.isDirectory()) {
                collectFilesInDirRecurse(file, walkedFiles)
            } else {
                walkedFiles.add(file)
            }
        }
    }


    override fun close() {
        scope.cancel()
    }
}