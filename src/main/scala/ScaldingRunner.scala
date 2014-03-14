import com.twitter.algebird.monad.ReaderFn
import com.twitter.scalding.{ Hdfs, TextLine, Job, Args }
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.scalding.{ InitialBatchedStore, Scalding }
import com.twitter.summingbird.scalding.state.HDFSState
import com.twitter.summingbird.scalding.store.{VersionedBatchStore, VersionedStore}
import com.twitter.summingbird.{Platform, Producer}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.serializer.Serialization



object ScaldingRunner {

  final val MillisInHour = 60 * 60 * 1000

  final val JobDir = "/var/summingbird/scaldingtest/obh"
  
  import Serialization._, StatusStreamer._

  val now = System.currentTimeMillis
  val waitingState = HDFSState(JobDir + "/waitstate", startTime = Some(Timestamp(now - 2 * MillisInHour)),
    numBatches = 3)
  
  val src = Producer.source[Scalding, String](Scalding.pipeFactoryExact(_ => TextLine(JobDir + "/input.txt")))
  //val versionedStore = VersionedStore[String, Long](JobDir + "/store")
  val versionedStore = VersionedBatchStore[String, Long, String, Long]("myStorePath", 3)(null)(identity)
  val store = new InitialBatchedStore(batcher.currentBatch - 2L, versionedStore)
  val mode = Hdfs(false, new Configuration())
    
  def main(args: Array[String]) {
    val job =  Scalding("wordcountJob")
    job.run(waitingState, mode, job.plan(wordCount[Scalding](src, store)))

    // Lookup results
    lookup()
  } 

  def lookup() {
    println("\nRESULTS: \n")

    val results = store.readLast(StatusStreamer.batcher.currentBatch, mode)
    println(results)
  } 
}