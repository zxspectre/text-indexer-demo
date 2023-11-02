package text.indexer.demo.lib.impl

fun Long.mbSizeString(): String {
    return "" + (this / 1024 / 1024) + "MB"
}