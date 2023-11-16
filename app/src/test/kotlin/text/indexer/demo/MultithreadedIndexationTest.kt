package text.indexer.demo

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import text.indexer.demo.SingleIndexationTest.Companion.waitForIndexToBeEmpty
import text.indexer.demo.SingleIndexationTest.Companion.waitForIndexationToFinish
import text.indexer.demo.lib.IndexerServiceFactory
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.Path
import kotlin.io.path.createDirectory
import kotlin.io.path.deleteRecursively
import kotlin.io.path.pathString
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals

private val log: Logger = LoggerFactory.getLogger(SingleIndexationTest::class.java)

@OptIn(ExperimentalPathApi::class)
class MultithreadedIndexationTest {
    private val createFilesJob = Job()
    private val createFilesCoroutineScope = CoroutineScope(createFilesJob)
    private val root = "src/test/resources/testfiles"

    fun assertOneFileFound(results: Collection<String>, expectedFileName: String) {
        assertEquals(1, results.size)
        assertEquals(expectedFileName, results.first())
    }

    fun createTempDir(): Path {
        val tempDir = Path("$root/${UUID.randomUUID()}")
        tempDir.createDirectory()
        return tempDir
    }

    suspend fun waitForChildrenTestsToFinish() {
        SingleIndexationTest.waitForCondition(5000) {
            createFilesJob.children.filter { !it.isCompleted }.count() == 0
            //not optimal
        }
    }


    @Test
    fun multithreadedSearchTest() {
        runBlocking { //replace runBlocking with runTests after working around skipped delays
            val tempDir = createTempDir()
            val indexerService = IndexerServiceFactory.wordExtractingIndexerService()
            val innerExceptions = ArrayList<Throwable>()
            val exceptionHandler = CoroutineExceptionHandler { _, exception ->
                innerExceptions.add(exception)
            }

            try {
                val counter = AtomicInteger(100)
                val repeatTimes = 100
                repeat(repeatTimes) {
                    createFilesCoroutineScope.launch(exceptionHandler) {
                        val number = counter.incrementAndGet()
                        //create unique files and index them
                        val newFile = Path(tempDir.pathString, "digit${number}.txt")
                        newFile.writeText("This is file number $number")
                        //search for unique entry present in only 1 file
                        indexerService.index(newFile)
                        waitForIndexationToFinish(indexerService)
                        assertOneFileFound(indexerService.search("" + number), newFile.pathString)
                        val currWords = indexerService.getIndexedWordsCnt()

                        //add dir including already indexed files
                        indexerService.index(root)
                        SingleIndexationTest.waitForCondition(5000) { // reduce FSPoller interval
                            indexerService.getInprogressFiles() == 0 && indexerService.getIndexedWordsCnt() > currWords
                        }
                        //still should have one file found per word
                        assertOneFileFound(indexerService.search("" + number), newFile.pathString)
                        assertOneFileFound(indexerService.search("Englishman"), "$root/small.rtf")
                    }
                }
                waitForChildrenTestsToFinish()
                if (innerExceptions.size > 0) {
                    throw innerExceptions.first()
                    // There should be a better way, use async?
                }
                assertEquals(repeatTimes + 242, indexerService.getIndexedWordsCnt())
                assertEquals(repeatTimes, indexerService.search("This").size)
                assertEquals(100 + repeatTimes, counter.get())
            } finally {
                indexerService.close()
                tempDir.deleteRecursively()
            }
        }
    }


    @Test
    fun multithreadedIndexUnindexTest() {
        runBlocking {
            val tempDir = createTempDir()
            val indexerService = IndexerServiceFactory.wordExtractingIndexerService()
            val innerExceptions = ArrayList<Throwable>()
            val exceptionHandler = CoroutineExceptionHandler { _, exception ->
                innerExceptions.add(exception)
            }

            try {
                val counter = AtomicInteger(100)
                val repeatTimes = 100
                repeat(repeatTimes) {
                    createFilesCoroutineScope.launch(exceptionHandler) {
                        val number = counter.incrementAndGet()
                        //create unique files and index them
                        val newFile = Path(tempDir.pathString, "digit${number}.txt")
                        newFile.writeText("This is file number $number")
                        //search for unique entry present in only 1 file
                        indexerService.index(newFile)
                        waitForIndexationToFinish(indexerService)
                        assertOneFileFound(indexerService.search("" + number), newFile.pathString)

                        //remove every file from index
                        indexerService.unindex(newFile)
                        waitForIndexToBeEmpty(indexerService)
                        //still should have one file found per word
                        assertEquals(0, indexerService.getIndexedWordsCnt())
                        assertEquals(0, indexerService.search("" + number).size)
                    }
                }
                waitForChildrenTestsToFinish()
                if (innerExceptions.size > 0) {
                    throw innerExceptions.first()
                    // There should be a better way, use async?
                }
                assertEquals(0, indexerService.getIndexedWordsCnt())
                assertEquals(100 + repeatTimes, counter.get())
            } finally {
                indexerService.close()
                tempDir.deleteRecursively()
            }
        }
    }
}