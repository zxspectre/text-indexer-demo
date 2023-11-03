package text.indexer.demo.lib.impl.storage

import java.nio.file.Path

class TrieStorage:ReverseIndexStorage<String, Path>  {
    override fun size(): Int {
        TODO("Not yet implemented")
    }

    override fun remove(document: Path) {
        TODO("Not yet implemented")
    }

    override fun get(keyword: String): Collection<Path> {
        TODO("Not yet implemented")
        //TODO improve search traverse?
    }

    override fun put(keyword: String, document: Path) {
        TODO("Not yet implemented")
    }
}