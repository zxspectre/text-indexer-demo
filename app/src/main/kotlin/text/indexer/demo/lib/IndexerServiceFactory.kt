package text.indexer.demo.lib

import text.indexer.demo.lib.impl.IndexerService

class IndexerServiceFactory {
    companion object {
        /**
         * Standard word extractor that will split the text by any whitespace characters
         * or punctuation marks.
         * 966Mb -> 5 sec
         */
        fun wordExtractingIndexerService(): IndexerService {
            return IndexerService(null, null)
        }


        /**
         * !Warning: slowest impl.
         * Word extractor that will split the text into words
         * based on specified delimiter Pattern (should be a valid java.util.regex.Pattern pattern).
         * Note that if you include only whitespace character, then punctuation marks will be part
         * of the extracted words.
         * DEBUG DemoApp - Using 611MB, indexed 2148614 words, inprogress=0MB
         * 966Mb -> 30 sec
         */
        fun delimiterBasedIndexerService(
            delimiterPattern: String
        ): IndexerService {
            return IndexerService(delimiterPattern, null)
        }


        /**
         * Will extract words using a specified by lambda tokenizer that accepts text file line
         * as an input and outputs a list of words
         * 966Mb -> 5 sec
         */
        fun lambdaTokenizerIndexerService(
            tokenizer: (String) -> Sequence<String>
        ): IndexerService {
            return IndexerService(null, tokenizer)
        }


        /**
         * In case you want a tokenizer that will work not on text lines as an input, but parts of the text
         * file delimited by something else.
         * E.g. a large one-liner text file.
         */
        fun lambdaTokenizerWithCustomLinesIndexerService(
            delimiterPattern: String,
            tokenizer: (String) -> Sequence<String>
        ): IndexerService {
            return IndexerService(delimiterPattern, tokenizer)
        }


    }
}