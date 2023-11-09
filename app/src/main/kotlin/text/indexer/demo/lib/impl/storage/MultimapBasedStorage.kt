package text.indexer.demo.lib.impl.storage

import com.google.common.collect.Multimaps
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

/**
 * Multimap based storage
 * For 13Mb of text files:
 * DEBUG DemoApp - Using 21MB, indexed 58169 words
 * DEBUG DemoApp - Using 19MB, indexed 50086 words case insensitive
 *
 * For 968Mb of text files (wiki dump):
 * DEBUG DemoApp - Using 610MB, indexed 2148614 words - 44sec
 */
class MultimapBasedStorage : ReverseIndexStorage<String, Path> {
    private var wordToFileMap =
        Multimaps.newSetMultimap(ConcurrentHashMap<String, Collection<Path>>()) { //TODO replace this multimap: overhead and not thread safe
            ConcurrentHashMap.newKeySet()
        }

    // Thread safe
    override fun size(): Int {
        return wordToFileMap.keySet().size
    }

    // Not thread safe
    override fun remove(documents: Set<Path>) {
        wordToFileMap.keySet().forEach {
            val curDocs = wordToFileMap[it]
            curDocs.removeAll(documents)
            if (curDocs.isEmpty()) {
                wordToFileMap.removeAll(it)
            }

        }
    }

    // Thread safe
    override fun get(keyword: String): Collection<Path> {
        return wordToFileMap[keyword]
    }

    // Thread safe
    override fun put(keyword: String, document: Path) {
        wordToFileMap.put(keyword, document)
    }
}