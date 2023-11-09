package text.indexer.demo.lib.impl.parser

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.file.Files
import java.nio.file.Path
import java.util.Scanner

/**
 * if (tokenizer == null && customDelimiter == null) -> "Standard word extractor"
 * if (tokenizer == null && customDelimiter != null) -> "Custom delimiter extractor"
 * if (tokenizer != null && customDelimiter == null) -> "Tokenizer that iterates on lines")
 * if (tokenizer != null && customDelimiter != null) -> "Tokenizer that iterates on custom tokens")
 */
class DocumentProcessor(
    private val tokenizer: ((String) -> Collection<String>)?,
    private val customDelimiter: String?,
    private val wordCallback: (String, Path) -> Unit
) {
    private val defaultRegex = Regex("[\\p{Punct}\\s]+")
    private val BULK_SIZE = 10000 //TODO make it dynamic?

    suspend fun extractWords(file: Path) {
        withContext(Dispatchers.IO) {
            var wordsToSave = HashSet<String>()
            if (customDelimiter != null) {
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
                        if (wordsToSave.size > BULK_SIZE) {//TODO deduplicate
                            wordsToSave.forEach { w -> wordCallback.invoke(w, file) }
                            //TODO run this in coroutine to parallelize
                            //TODO check no blocking ops - thread starvation?
                            wordsToSave = HashSet()
                        }
                    }
                }
            } else {
                Files.newBufferedReader(file).use {
                    val lineSplitter = tokenizer ?: { s: String -> defaultTokenizer(s) }
                    while (it.ready()) {
                        val line = it.readLine()
                        val words = lineSplitter.invoke(line)
                        wordsToSave.addAll(words)
                        if (wordsToSave.size > BULK_SIZE) {//TODO deduplicate
                            wordsToSave.forEach { w -> wordCallback.invoke(w, file) }
                            wordsToSave = HashSet()
                        }
                    }
                }
            }
            if(wordsToSave.isNotEmpty()){
                wordsToSave.forEach { w -> wordCallback.invoke(w, file) } //TODO dedupl
            }
        }
    }

    private fun applyTokenizerAndProcessWords(
        tokenizer: ((String) -> Collection<String>),
        word: String,
        filePath: Path
    ) {
        tokenizer.invoke(word).forEach {

            wordCallback.invoke(it, filePath)
        }
    }

    private fun defaultTokenizer(line: String): Collection<String> {
        return line.split(defaultRegex)
    }


}