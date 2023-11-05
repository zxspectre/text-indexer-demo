package text.indexer.demo.lib.impl.fswatcher

import java.nio.file.Path


enum class RequestType {
    NEW_FILE, NEW_DIR, DELETE
}

class IndexRequest(val type: RequestType, val path: Path) {
}