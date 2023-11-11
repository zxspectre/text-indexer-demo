package text.indexer.demo.lib

import text.indexer.demo.lib.impl.IndexerService

class IndexerServiceFactory {
    companion object {
        /**
         * Standard word extractor that will split the text by any whitespace characters or punctuation marks.
         * Equivalent to {@code IndexerServiceFactory.lambdaTokenizerIndexerService { s: String -> s.splitToSequence(regex)} }
         * with precompiled regex equal to {@code Regex("[\\p{Punct}\\s]++")}.
         *
         * Uses BufferedReader
         *
         * Local benchmark: 966Mb files -> 5 sec
         */
        fun wordExtractingIndexerService(): IndexerService {
            return IndexerService(null, null)
        }



        /**
         * Will extract words using a specified by lambda tokenizer that accepts text file line
         * as an input and outputs a list of words. Generic case of [wordExtractingIndexerService]
         *
         * Uses BufferedReader
         *
         * 966Mb -> 5 sec
         */
        fun lambdaTokenizerIndexerService(
            tokenizer: (String) -> Sequence<String>
        ): IndexerService {
            return IndexerService(null, tokenizer)
        }


        /**
         * !Warning: slowest impl (even to the point of being pointless)
         * Will use [java.util.Scanner] with specified delimiter pattern.
         * Extracts words one by one by calling [java.util.Scanner.next]
         *
         * Local benchmark: 966Mb -> 30 sec
         */
        fun delimiterBasedIndexerService(
            delimiterPattern: String
        ): IndexerService {
            return IndexerService(delimiterPattern, null)
        }


        /**
         * !Warning: slow impl.
         * In case you want a tokenizer that will work not on text lines as an input, but parts of the text
         * file delimited by something else.
         * E.g. a large one-liner text file.
         *
         * Uses [java.util.Scanner]
         *
         * Performance will be better if specified delimiter extracts larger portions of the file
         * to be tokenized next by the specified tokenizer
         */
        fun lambdaTokenizerWithCustomLinesIndexerService(
            delimiterPattern: String,
            tokenizer: (String) -> Sequence<String>
        ): IndexerService {
            return IndexerService(delimiterPattern, tokenizer)
        }


    }
}