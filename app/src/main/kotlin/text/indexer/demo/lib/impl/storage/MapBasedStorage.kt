package text.indexer.demo.lib.impl.storage

import com.google.common.collect.Multimaps
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

class MapBasedStorage:ReverseIndexStorage<String, Path> {
    private var wordToFileMap = Multimaps.newSetMultimap(ConcurrentHashMap<String, Collection<Path>>()) {
        ConcurrentHashMap.newKeySet()
    }
    private var deletedIndexedFiles = HashSet<Path>()

    //TODO scope.launch {periodically update wordToFileMap with deletedIndexedFiles}

    // Thread safe
    override fun size(): Int {
        return wordToFileMap.size()
    }

    // Not thread safe
    override fun remove(document: Path) {
        deletedIndexedFiles.add(document)
    }

    // Thread safe
    override fun get(keyword: String): Collection<Path> {
        return wordToFileMap.get(keyword)
            .filter { !deletedIndexedFiles.contains(it) } // TODO this implementation doesn't shrink
    }

    // Thread safe
    override fun put(keyword: String, document: Path) {
        wordToFileMap.put(keyword, document)
    }
}