package text.indexer.demo.lib

import text.indexer.demo.lib.impl.IndexerService

class IndexerServiceFactory {
    companion object {
        /**
         * Standard word extractor that will split the text by any whitespace characters
         * or punctuation marks.
         */
        fun wordExtractingIndexerService(): IndexerService {
            return IndexerService(null, null)
        }


        /**
         * Word extractor that will split the text into words
         * based on specified delimiter Pattern (should be a valid java.util.regex.Pattern pattern).
         * Note that if you include only whitespace character, then punctuation marks will be part
         * of the extracted words.
         */
        fun delimiterBasedIndexerService(
            delimiterPattern: String
        ): IndexerService {
            return IndexerService(delimiterPattern, null)
        }


        /**
         * Will extract words using a specified by lambda tokenizer that accepts text file line
         * as an input and outputs a list of words
         */
        fun lambdaTokenizerIndexerService(
            tokenizer: (String) -> List<String>
        ): IndexerService {
            //TODO change List<String> to Sequence<String>
            return IndexerService(null, tokenizer)
        }


        /**
         * In case you want a tokenizer that will work not on text lines as an input, but parts of the text
         * file delimited by something else.
         * E.g. a large one-liner text file.
         */
        fun lambdaTokenizerWithCustomLinesIndexerService(
            delimiterPattern: String,
            tokenizer: (String) -> List<String>
        ): IndexerService {
            return IndexerService(delimiterPattern, tokenizer)
        }


    }
}