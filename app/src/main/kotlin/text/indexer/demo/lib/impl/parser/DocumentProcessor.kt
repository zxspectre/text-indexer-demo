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
private const val BULK_SIZE = 10000 //TODO make it dynamic?

class DocumentProcessor(
    coroutineContext: CoroutineContext,
    private val tokenizer: ((String) -> Collection<String>)?,
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
        var wordsToSave = HashSet<String>()
        Files.newBufferedReader(file).use {
            val lineSplitter = tokenizer ?: { s: String -> defaultTokenizer(s) }
            while (it.ready()) {
                val line = it.readLine()
                val words = lineSplitter.invoke(line)
                wordsToSave.addAll(words)
                wordsToSave = batchIntoCallback(wordsToSave, file, false)
            }
        }
        batchIntoCallback(wordsToSave, file, true)
    }

    private fun processFileWithScanner(file: Path) {
        var wordsToSave = HashSet<String>()
        Scanner(file).use {
            it.useDelimiter(customDelimiter)
            while (it.hasNext()) {
                val delimitedText = it.next()
                if (tokenizer != null) {
                    //TODO is it possible to cache processed words to make things faster?
                    wordsToSave.addAll(tokenizer.invoke(delimitedText)) //TODO can I split string into set, not list?
                } else {
                    wordsToSave.add(delimitedText)
                }
                wordsToSave = batchIntoCallback(wordsToSave, file, false)
            }
        }
        batchIntoCallback(wordsToSave, file, true)
    }

    private fun batchIntoCallback(
        wordsToSave: java.util.HashSet<String>,
        file: Path,
        force: Boolean
    ): java.util.HashSet<String> {
        if (wordsToSave.size > BULK_SIZE || force) {
            documentProcessorCoroutineScope.launch {
                wordsToSave.forEach { w -> wordCallback.invoke(w, file) }
            }
            //TODO check no blocking ops up by stack - could be threads starvation issue?
            return HashSet()
        }
        return wordsToSave
    }

    private fun defaultTokenizer(line: String): Collection<String> {
        return line.split(defaultRegex)
    }


}