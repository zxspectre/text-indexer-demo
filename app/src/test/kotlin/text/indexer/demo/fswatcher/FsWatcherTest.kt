package text.indexer.demo.fswatcher

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import text.indexer.demo.lib.impl.fswatcher.EventType
import text.indexer.demo.lib.impl.fswatcher.FSEvent
import text.indexer.demo.lib.impl.fswatcher.FsWatcher
import java.nio.file.Files
import java.util.UUID
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.Path
import kotlin.io.path.deleteIfExists
import kotlin.io.path.deleteRecursively
import kotlin.io.path.pathString
import kotlin.io.path.writeText
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

private val log: Logger = LoggerFactory.getLogger(FsWatcherTest::class.java)

class FsWatcherTest {

    suspend fun waitForPoll(watchService: FsWatcher) {
        delay(watchService.pollingIntervalMillis + 150)
    }


    fun assertEventHasTypeAndFiles(
        watchService: FsWatcher,
        eventType: EventType,
        filesInEvent: Int,
        checkNoOtherEventsInChannel: Boolean = true
    ): FSEvent {
        val fsEvent = watchService.fileEventChannel.tryReceive().getOrThrow()
        // ^-use smth better like receive w/ timeout
        assertEquals(eventType, fsEvent.type)
        assertEquals(filesInEvent, fsEvent.files.size)
        if (checkNoOtherEventsInChannel) {
            assertTrue(watchService.fileEventChannel.tryReceive().isFailure)
        }
        return fsEvent
    }

    fun assertReceivedNewAndDeleteEvents(watchService: FsWatcher){
        var fsEvent1 = watchService.fileEventChannel.tryReceive().getOrThrow()
        var fsEvent2 = watchService.fileEventChannel.tryReceive().getOrThrow()
        assertNotEquals(fsEvent1.type, fsEvent2.type)
        assertEquals(1, fsEvent1.files.size)
        assertEquals(1, fsEvent2.files.size)
    }


    @Test
    fun testWatchAndUnwatch() {
        runBlocking {
            val watchService = FsWatcher(300)
            try {
                watchService.init()
                // start polling two files and dir with 2 files
                watchService.watch(Path("src/test/resources/testfiles/inner"))
                watchService.watch(Path("src/test/resources/testfiles/ipsum.rtf"))
                watchService.watch(Path("src/test/resources/testfiles/small.rtf"))
                waitForPoll(watchService)
                // check that we receive event with type 'NEW' file and '4' files
                assertEventHasTypeAndFiles(watchService, EventType.NEW, 4)

                // try to remove file that was not added, but was included in the directory
                watchService.unwatch(Path("src/test/resources/testfiles/inner/quasi.rtf"))
                waitForPoll(watchService)
                // no entries were marked for deletion
                assertTrue(watchService.fileEventChannel.tryReceive().isFailure)

                // remove 1 file
                watchService.unwatch(Path("src/test/resources/testfiles/ipsum.rtf"))
                waitForPoll(watchService)
                var fsEvent = assertEventHasTypeAndFiles(watchService, EventType.DELETED, 1)
                assertEquals("src/test/resources/testfiles/ipsum.rtf", fsEvent.files.first().pathString)

                // remove dir
                watchService.unwatch(Path("src/test/resources/testfiles/inner"))
                waitForPoll(watchService)
                fsEvent = assertEventHasTypeAndFiles(watchService, EventType.DELETED, 2)
                assertTrue(fsEvent.files.first().pathString.contains("/inner"))
            } finally {
                watchService.close()
            }
        }
    }


    @OptIn(ExperimentalPathApi::class)
    @Test
    fun testDirChangesByFs() {
        runBlocking {
            val watchService = FsWatcher(300)
            val tempDir = UUID.randomUUID().toString()
            val newFile = Path("src/test/resources/testfiles/${tempDir}/newFile.txt")
            val newFile2 = Path("src/test/resources/testfiles/${tempDir}/inner/newFile2.txt")
            try {
                watchService.init()
                // create empty dir, watch it, no NEW events
                Files.createDirectories(newFile.parent)  // replace by non blocking kotlin alt
                watchService.watch(newFile.parent)
                waitForPoll(watchService)
                assertTrue(watchService.fileEventChannel.tryReceive().isFailure)

                // write contents to file, get one NEW event
                newFile.writeText("Test contents. This will be deleted. Probably...")
                waitForPoll(watchService)
                assertEventHasTypeAndFiles(watchService, EventType.NEW, 1)

                // update file contents, get two events: DELETE + NEW
                newFile.writeText("some text")
                waitForPoll(watchService)
                assertReceivedNewAndDeleteEvents(watchService)

                // create second nested file, delete initial file
                newFile.deleteIfExists()
                Files.createDirectories(newFile2.parent)
                newFile2.writeText("another file in subdir")
                waitForPoll(watchService)
                assertReceivedNewAndDeleteEvents(watchService)

            } finally {
                watchService.close()
                newFile.parent.deleteRecursively()
            }
        }
    }


}