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
    private val tokenizer: ((String) -> List<String>)?,
    private val customDelimiter: String?,
    private val wordCallback: (String, Path) -> Unit
) {

    suspend fun extractWords(file: Path) {
        withContext(Dispatchers.IO) {
            if (customDelimiter != null) {
                Scanner(file).use {
                    it.useDelimiter(customDelimiter)
                    while (it.hasNext()) {
                        val delimitedText = it.next()
                        if (tokenizer != null) {
                            applyTokenizerAndProcessWords(tokenizer, delimitedText, file)
                        } else {
                            wordCallback.invoke(delimitedText, file)
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
        }
    }

    private fun applyTokenizerAndProcessWords(tokenizer: ((String) -> List<String>)?, word: String, filePath: Path) {
        tokenizer!!.invoke(word).forEach { wordCallback.invoke(it, filePath) }
    }

    private fun defaultTokenizer(line: String): List<String> {
        return line.split("\\s+".toRegex()).map { it.replace("""^\p{Punct}+|\p{Punct}+$""".toRegex(), "") }
    }


}