package text.indexer.demo.lib.impl.fswatcher

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries


class FSPollingWatcher : Closeable {
    private val PUBLISH_WATCHED_FILE_SETS_INTERVAL_MS = 2000L
    private val log: Logger = LoggerFactory.getLogger(FSPollingWatcher::class.java)

    private val pollingIntervalMillis = 1000L

    private val mutableWatchedFiles = HashSet<Path>()
    private val mutableWatchedDirs = HashSet<Path>()

    private var readonlyWatchedFiles = HashSet<Path>()
    private var readonlyWatchedDirs = HashSet<Path>()

    // no concurrent access
    private val fileModificationTimestamps = HashMap<Path, Long>()
    private var watchedDirFiles = HashSet<Path>()


    private val scope = CoroutineScope(Dispatchers.Default)
    private val watchRequestChannel = Channel<WatchRequest>(Channel.BUFFERED)
    val fileModifiedChannel = Channel<Path>(Channel.BUFFERED)
    val fileDeletedChannel = Channel<Path>(Channel.BUFFERED)


    suspend fun watch(path: Path) {
        require(path.exists()) { "Path does not exist" } //or have two methods or create separate 'waiting' list
        val reqType = when{
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
            var lastChangePublished = System.currentTimeMillis()
            while (scope.isActive){
                val msg = watchRequestChannel.tryReceive()
                if(msg.isSuccess) {
                    updateWatchedFilesDirs(msg)
                }
                if(System.currentTimeMillis() - lastChangePublished > PUBLISH_WATCHED_FILE_SETS_INTERVAL_MS){
                    publishWatchedFilesDirs()
                    lastChangePublished = System.currentTimeMillis()
                }
                if(msg.isFailure){
                    delay(pollingIntervalMillis)
                }
            }
        }
    }

    private fun publishWatchedFilesDirs() {
        readonlyWatchedFiles = HashSet(mutableWatchedFiles)
        readonlyWatchedDirs = HashSet(mutableWatchedDirs)
    }

    private fun updateWatchedFilesDirs(msg: ChannelResult<WatchRequest>) {
        val watchMsg = msg.getOrThrow()
        when (watchMsg.type) {
            RequestType.NEW_FILE -> mutableWatchedFiles.add(watchMsg.path)
            RequestType.NEW_DIR -> mutableWatchedDirs.add(watchMsg.path)
            RequestType.DELETE -> mutableWatchedFiles.remove(watchMsg.path) && mutableWatchedDirs.remove(watchMsg.path)
        }
    }


    private fun pollForFsChanges() {
        scope.launch {
            delay(100) //small pause after creation if some files are immediately added
            while (scope.isActive) {
                log.debug("polling FS")
                // Handle changes to watched directories
                val currentDirFiles = HashSet<Path>()
                readonlyWatchedDirs.forEach { collectFilesInDirRecurse(it, currentDirFiles) }
                var deletedDirFiles = watchedDirFiles.subtract(currentDirFiles)
                deletedDirFiles = deletedDirFiles.stream()
                    .filter {
                        // Files that disappeared while scanning for dirs could be deleted or encapsulating dir was
                        // removed. Either way we shouldn't remove such files if they are watched separately as a file
                        !readonlyWatchedFiles.contains(it)
                    }
                    .collect(Collectors.toSet())

                //handle changes to watched files
                val deletedFiles = HashSet<Path>()
                val mayBeModifiedFiles = currentDirFiles
                readonlyWatchedFiles.forEach {
                    if (it.exists()) {
                        mayBeModifiedFiles.add(it)
                    } else {
                        deletedFiles.add(it)
                    }
                }

                //handle deleted files
                deletedFiles.addAll(deletedDirFiles)
                deletedFiles.forEach {
                    log.debug("notify channel about deleted file $it")
                    fileModificationTimestamps.remove(it)
                    fileDeletedChannel.send(it)
                }
                //handle mayBeModifiedFiles
                val modifiedFiles = HashSet<Path>()
                mayBeModifiedFiles.forEach {
                    val lastModified = Files.getLastModifiedTime(it).toMillis()
                    if (fileModificationTimestamps[it] != lastModified) {
                        fileModificationTimestamps[it] = lastModified
                        modifiedFiles.add(it)
                    }
                }
                modifiedFiles.forEach {
                    log.debug("notify channel about modified file $it")
                    fileDeletedChannel.send(it) /TODO is it okay to fully update?
                    fileModifiedChannel.send(it)
                }

                watchedDirFiles = currentDirFiles

                delay(pollingIntervalMillis)
            }
            log.info("Terminating poller loop")
            fileModifiedChannel.close()
            fileDeletedChannel.close()
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