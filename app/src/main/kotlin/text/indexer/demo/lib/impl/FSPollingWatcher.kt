package text.indexer.demo.lib.impl

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
import java.util.concurrent.CopyOnWriteArraySet
import java.util.stream.Collectors
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries

private val log: Logger = LoggerFactory.getLogger(FSPollingWatcher::class.java)


class FSPollingWatcher : Closeable {
    //TODO this should use coroutine with channel
    private val pollingIntervalMillis = 5000L
    private val watchedFiles = CopyOnWriteArraySet<Path>() //TODO check better collection impl
    private val watchedDirs = CopyOnWriteArraySet<Path>()

    // no concurrent access
    private val fileModificationTimestamps = HashMap<Path, Long>()
    private var watchedDirFiles = HashSet<Path>()
    //TODO ^ v change File to Path


    private val scope = CoroutineScope(Dispatchers.Default)
    val fileModifiedChannel = Channel<Path>(Channel.BUFFERED)
    val fileDeletedChannel = Channel<Path>(Channel.BUFFERED)


    init {
        pollForChanges()
    }


    private fun pollForChanges() {
        scope.launch {
            delay(100)
            while (scope.isActive) {
                log.debug("polling FS")
                // Handle changes to watched directories
                val currentDirFiles = HashSet<Path>()
                watchedDirs.forEach { scanDirForChanges(it, currentDirFiles) }
                var deletedDirFiles = watchedDirFiles.subtract(currentDirFiles)
                deletedDirFiles = deletedDirFiles.stream()
                    .filter {
                        // Files that disappeared while scanning for dirs could be deleted or encapsulating dir was
                        // removed. Either way we shouldn't remove such files if they are watched separately as a file
                        !watchedFiles.contains(it)
                    }
                    .collect(Collectors.toSet())

                //handle changes to watched files
                val deletedFiles = HashSet<Path>()
                val mayBeModifiedFiles = currentDirFiles
                watchedFiles.forEach {
                    if (it.exists()) {
                        mayBeModifiedFiles.add(it)
                    } else {
                        deletedFiles.add(it)
                    }
                }

                //handle deleted files
                deletedFiles.addAll(deletedDirFiles)
                deletedFiles.forEach {
                    log.debug("notify channel to remove file $it from index")
                    fileModificationTimestamps.remove(it)
                    fileDeletedChannel.send(it)
                }
                //handle mayBeModifiedFiles
                val filesToReindex = HashSet<Path>()
                mayBeModifiedFiles.forEach {
                    val lastModified = Files.getLastModifiedTime(it).toMillis()
                    if (fileModificationTimestamps[it] != lastModified) {
                        fileModificationTimestamps[it] = lastModified
                        filesToReindex.add(it)
                    }
                }
                filesToReindex.forEach {
                    log.debug("notify channel to index file $it")
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

    private fun scanDirForChanges(directory: Path, walkedFiles: MutableSet<Path>) {
        val files = directory.listDirectoryEntries() ?: return
        for (file in files) {
            if (file.isDirectory()) {
                scanDirForChanges(file, walkedFiles)
            } else {
                walkedFiles.add(file)
            }
        }
    }

    private fun addDirToWatch(dir: Path) {
        watchedDirs.add(dir)
    }

    private fun addFileToWatch(file: Path) {
        watchedFiles.add(file)
    }


    fun register(path: Path) {
        if (path.isDirectory()) {
            addDirToWatch(path)
        } else {
            addFileToWatch(path)
        }
    }

    override fun close() {
        scope.cancel()
    }
}
