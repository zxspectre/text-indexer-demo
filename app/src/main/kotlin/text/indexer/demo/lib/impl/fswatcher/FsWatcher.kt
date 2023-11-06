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
import java.util.concurrent.locks.ReentrantLock
import java.util.stream.Collectors
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries


class FsWatcher : Closeable {
    val fileEventChannel = Channel<FSEvent>(Channel.BUFFERED)

    private val watchRequestChannel = Channel<WatchRequest>(Channel.BUFFERED)

    private val log: Logger = LoggerFactory.getLogger(FsWatcher::class.java)

    private val pollingIntervalMillis = 2000L

    private val watchedFilesLock = ReentrantLock()
    private val mutableWatchedFiles = HashSet<Path>()
    private val mutableWatchedDirs = HashSet<Path>()

    // no concurrent access
    private val fileModificationTimestamps = HashMap<Path, Long>()

    private var watchedDirFiles = HashSet<Path>()
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
        watchedFilesLock.run {
            when (watchMsg.type) {
                RequestType.NEW_FILE -> mutableWatchedFiles.add(watchMsg.path)
                RequestType.NEW_DIR -> mutableWatchedDirs.add(watchMsg.path)
                RequestType.DELETE -> mutableWatchedFiles.remove(watchMsg.path) && mutableWatchedDirs.remove(watchMsg.path)
            }
        }
    }


    private fun pollForFsChanges() {
        scope.launch {
            delay(100) //small pause after creation if some files are immediately added
            val watchedFiles: Set<Path>
            val watchedDirs: Set<Path>
            watchedFilesLock.run {
                watchedFiles = HashSet(mutableWatchedFiles)
                watchedDirs = HashSet(mutableWatchedDirs)
            }

            while (scope.isActive) {
                log.trace("polling FS")
                // Handle changes to watched directories
                val currentDirFiles = HashSet<Path>()
                watchedDirs.forEach { collectFilesInDirRecurse(it, currentDirFiles) }
                val deletedDirFiles = watchedDirFiles.subtract(currentDirFiles)

                val deletedFiles = HashSet<Path>()
                deletedFiles.addAll(deletedDirFiles.stream()
                    .filter {
                        // Files that disappeared while scanning for dirs could be deleted or encapsulating dir was
                        // removed. Either way we shouldn't remove such files if they are watched separately as a file
                        !watchedFiles.contains(it)
                    }
                    .collect(Collectors.toSet()))

                val mayBeModifiedFiles = HashSet<Path>(currentDirFiles)
                watchedFiles.forEach {
                    if (it.exists()) {
                        mayBeModifiedFiles.add(it)
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
                mayBeModifiedFiles.forEach {
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

                watchedDirFiles = currentDirFiles

                delay(pollingIntervalMillis)
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