package text.indexer.demo.lib.impl.parser

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.nio.file.Files
import java.nio.file.Path
import java.util.Scanner
import kotlin.coroutines.CoroutineContext

/**
 * if (tokenizer == null && customDelimiter == null) -> "Standard word extractor"
 * if (tokenizer == null && customDelimiter != null) -> "Custom delimiter extractor"
 * if (tokenizer != null && customDelimiter == null) -> "Tokenizer that iterates on lines")
 * if (tokenizer != null && customDelimiter != null) -> "Tokenizer that iterates on custom tokens")
 */
private const val SEQUENCE_BULK_SIZE = 250
private const val STRING_BULK_SIZE = 10000

class DocumentProcessor(
    coroutineContext: CoroutineContext,
    private val tokenizer: ((String) -> Sequence<String>)?,
    private val customDelimiter: String?,
    private val wordCallback: (String, Path) -> Unit
) {
    private val defaultRegex = Regex("[\\p{Punct}\\s]+")
    private val documentProcessorCoroutineScope = CoroutineScope(coroutineContext)

    suspend fun extractWords(file: Path) {
        withContext(Dispatchers.IO) {
            if (customDelimiter != null) {
                processFileWithScanner(file)
            } else {
                processFileWithBufferedReader(file)
            }
        }
    }

    private fun processFileWithBufferedReader(file: Path) {
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

    private fun processFileWithScanner(file: Path) {
        var wordsToSave = ArrayList<String>()
        Scanner(file).use {
            it.useDelimiter(customDelimiter)
            while (it.hasNext()) {
                val delimitedText = it.next()
                wordsToSave.add(delimitedText)
                wordsToSave = batchStringIntoCallback(wordsToSave, tokenizer, file, false)
            }
        }
        batchStringIntoCallback(wordsToSave, tokenizer, file, true)
    }

    private fun batchStringIntoCallback(
        wordsToSave: java.util.ArrayList<String>,
        tokenizer: ((String) -> Sequence<String>)?,
        file: Path,
        force: Boolean
    ): java.util.ArrayList<String> {
        if (wordsToSave.size > STRING_BULK_SIZE || force) {
            if (tokenizer != null) {
                documentProcessorCoroutineScope.launch {
                    val wordSet = HashSet<String>()
                    wordsToSave.forEach { wordSet.addAll(tokenizer.invoke(it)) }
                    wordSet.forEach {
                        wordCallback.invoke(it, file)
                    }
                }
            } else {
                documentProcessorCoroutineScope.launch {
                    val wordSet = HashSet<String>()
                    wordSet.addAll(wordsToSave)
                    wordSet.forEach { wordCallback.invoke(it, file) }
                }
            }
            return ArrayList()
        }
        return wordsToSave
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
            //TODO check no blocking ops up by stack - could be threads starvation issue?
            return ArrayList()
        }
        return wordsSequencesToSave
    }

    private fun defaultTokenizer(line: String): Sequence<String> {
        return line.splitToSequence(defaultRegex)
    }

}