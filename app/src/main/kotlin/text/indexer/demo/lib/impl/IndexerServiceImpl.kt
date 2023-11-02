package text.indexer.demo.lib.impl

import com.google.common.collect.Multimaps
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.MainScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import text.indexer.demo.lib.IndexerService
import java.io.Closeable
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.util.Date
import java.util.Scanner
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet
import kotlin.io.path.Path
import kotlin.io.path.isDirectory
import kotlin.io.path.pathString

private const val MAX_WORD_LENGTH = 16384


class IndexerServiceImpl(delimiter: String?, private val tokenizer: ((String) -> List<String>)?)
    : IndexerService, Closeable {


    private val scope = CoroutineScope(Dispatchers.IO)
    private var wordToFileMap = Multimaps.newSetMultimap(ConcurrentHashMap<String, Collection<Path>>()) {
        ConcurrentHashMap.newKeySet()
    }
    private var deletedIndexedFiles = HashSet<Path>() // TODO replace w' some append specific concurrent set
    private var externallyMarkedForDeletionFiles = HashSet<Path>()

    private val customDelimiter: String? = delimiter
    private val watchService: FSPollingWatcher = FSPollingWatcher()


    init {
        val maxHeapMb = Runtime.getRuntime().maxMemory()/(1024*1024)
        println("Running with maxheap = $maxHeapMb MB")
        scope.launch {
            while (scope.isActive) {
//                println(Date().toString()+" check modified files")
                processFileModifiedEvent(watchService.fileModifiedChannel.receive())
            }
        }
        scope.launch {
            while (scope.isActive) {
                println(Date().toString()+" check deleted files")
                processFileDeletedEvent(watchService.fileDeletedChannel.receive())
            }
        }

        //TODO scope.launch {merge deletedIndexedFiles with externallyMarkedForDeletionFiles; update wordToFileMap with deletedIndexedFiles}
    }

    private suspend fun processFileModifiedEvent(file: Path) {
        index(file)
    }

    private suspend fun processFileDeletedEvent(file: Path) {
        println("Mark $file for deletion")
        deletedIndexedFiles.add(file)
    }

    override suspend fun index(path: String) {
        TODO("Not yet implemented")
    }

    override suspend fun unindex(path: String) {
        TODO("Not yet implemented")
        // if (is_file) externallyMarkedForDeletionFiles.add(it) else ???
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
        val filePath = Path(filePath)
        watchService.register(filePath)
        //TODO check TS before indexing
        //TODO concurrent modification of indexed file - ?
//        index(filePath)
        //TODO check and store TS after indexing
    }

    private suspend fun index(file: Path) {
        //TODO concurrent file modification while indexing?
        println(Date().toString() + " Indexing Start ${file.pathString}")
        if (file.isDirectory()) {
            throw IllegalArgumentException("Should specify file, but was directory: $file")
        }
        if (tokenizer == null && customDelimiter == null) {
//            println("Standard word extractor")
        } else if (tokenizer == null && customDelimiter != null) {
//            println("Custom delimiter extractor")
        } else if (tokenizer != null && customDelimiter == null) {
//            println("Tokenizer that iterates on lines")
        } else if (tokenizer != null && customDelimiter != null) {
//            println("Tokenizer that iterates on custom tokens")
        }

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
//        println("Indexing Done ${file.pathString}")
    }

    private fun defaultTokenizer(line: String): List<String> {
        return line.split("\\s+".toRegex()).map { it.replace("""^\p{Punct}+|\p{Punct}+$""".toRegex(), "") }
    }

    private fun applyTokenizerAndProcessWords(tokenizer: ((String) -> List<String>)?, word: String, filePath: Path) {
        tokenizer!!.invoke(word).forEach { processWord(it, filePath) }
    }

    private fun processWord(word: String, filePath: Path) {
        if (word.isNotEmpty()) {
            if(word.length>MAX_WORD_LENGTH){
                println("Ignoring word <${word.substring(0, 10)}...> with length ${word.length} in file $filePath, " +
                        "current limit is $MAX_WORD_LENGTH")
            }else {
                wordToFileMap.put(word, filePath)
            }
        }
    }

    override fun search(word: String): Collection<String> {
//        println("Searching for '$word'")
        val result = wordToFileMap[word]
        val res = result
            .filter { !deletedIndexedFiles.contains(it) }
            .map { it.pathString }
        println("Found '$word' in $res")
        return res
    }

    override fun close() {
        watchService.close()
        scope.cancel()
    }
}