package text.indexer.demo.lib.impl.fswatcher

import java.nio.file.Path


enum class RequestType {
    NEW_FILE, NEW_DIR, DELETE
}

data class WatchRequest(val type: RequestType, val path: Path) {
}