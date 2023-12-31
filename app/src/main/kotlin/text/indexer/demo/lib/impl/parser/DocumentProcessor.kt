package text.indexer.demo.lib.impl.parser

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import kotlin.coroutines.CoroutineContext

private const val SEQUENCE_BULK_SIZE = 250
private val log: Logger = LoggerFactory.getLogger(DocumentProcessor::class.java)

class DocumentProcessor(
    coroutineContext: CoroutineContext,
    private val tokenizer: ((String) -> Sequence<String>)?,
    private val wordCallback: (String, Path) -> Unit
) {
    private val defaultRegex = Regex("[\\p{Punct}\\s]+")
    private val loggingExceptionHandler = CoroutineExceptionHandler { _, e ->
        log.error("Exception in DocumentProcessor", e)
    }
    private val documentProcessorCoroutineScope = CoroutineScope(loggingExceptionHandler + coroutineContext)

    suspend fun extractWords(file: Path) {
        withContext(Dispatchers.IO) {
            var wordsToSave = ArrayList<Sequence<String>>()
            Files.newBufferedReader(file).use {
                val lineSplitter = tokenizer ?: { s: String -> defaultTokenizer(s) }
                while (it.ready()) {
                    val line = it.readLine()
                    wordsToSave.add(lineSplitter.invoke(line))
                    wordsToSave = batchSequenceIntoCallback(wordsToSave, file, false)
                }
            }
            batchSequenceIntoCallback(wordsToSave, file, true)
        }
    }

    private fun batchSequenceIntoCallback(
        wordsSequencesToSave: ArrayList<Sequence<String>>,
        file: Path,
        force: Boolean
    ): ArrayList<Sequence<String>> {
        if (wordsSequencesToSave.size > SEQUENCE_BULK_SIZE || force) {
            documentProcessorCoroutineScope.launch {
                val wordSet = HashSet<String>()
                wordsSequencesToSave.forEach { wordSet.addAll(it) }
                wordSet.forEach { w -> wordCallback.invoke(w, file) }
            }
            return ArrayList()
        }
        return wordsSequencesToSave
    }

    private fun defaultTokenizer(line: String): Sequence<String> {
        return line.splitToSequence(defaultRegex)
    }

}