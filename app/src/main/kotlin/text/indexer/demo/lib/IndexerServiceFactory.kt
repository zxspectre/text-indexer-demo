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
            return IndexerService(null)
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
            return IndexerService(tokenizer)
        }
    }
}