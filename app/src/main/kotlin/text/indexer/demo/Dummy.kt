package text.indexer.demo

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger


private val log: Logger = LoggerFactory.getLogger("D")
val getDispatcher =  Executors.newFixedThreadPool(1).asCoroutineDispatcher()
val handleChannelCoroutineScope = CoroutineScope(Dispatchers.Default)
val job = Job()
val indexFileCoroutineScope = CoroutineScope(getDispatcher+job)
var maintenance = false
suspend fun main() {


    handleChannelCoroutineScope.launch {
        val i = AtomicInteger(0)
        while (isActive) {
            i.incrementAndGet()
            if (!maintenance) {
                val cur = i.get()
                indexFileCoroutineScope.launch {
                    repeat(4) {
                        log.debug("Worker $cur ... $it")
                        delay(437)
                    }
                    log.debug("Worker $cur is done")
                }
            }
            delay(600)
        }
    }

    handleChannelCoroutineScope.launch {
        while (isActive) {
            maintain()
            //end
        }
    }
    delay(10000)
    indexFileCoroutineScope.cancel()
    handleChannelCoroutineScope.cancel()
    getDispatcher.close()
    log.debug("Done")
}

suspend fun maintain(){
    delay(700)
    //start
    maintenance = true
    while (job.children.filter { !it.isCompleted }.count()>0){
        delay(100)
    }
    repeat(4) {
        log.debug("Maintenance ...$it ")
        delay(500)
    }
    maintenance = false
}