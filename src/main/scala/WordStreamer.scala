// import com.twitter.summingbird.scalding.{ WaitingState, Scalding, ScaldingStore, PipeFactory }
// import com.twitter.scalding.{Local, TextLine}

import com.twitter.summingbird.scalding.store.InitialBatchedStore
import com.twitter.scalding.{TextLine, Hdfs, Job, Args}
import com.twitter.summingbird.scalding._
import java.util.Date
import com.twitter.algebird.Interval
import com.twitter.summingbird.batch._
import com.twitter.summingbird.batch.WaitingState
import com.twitter.summingbird.batch.RunningState
import com.twitter.summingbird.batch.PrepareState
import org.apache.hadoop.conf.Configuration
import com.twitter.summingbird.{Platform, Producer, TimeExtractor}
import scala.Right


// This is not really usable, just a mock that does the same state over and over
//class LoopState[T](init: Interval[T]) extends WaitingState[T] { self =>
//  def begin = new RunningState[T] {
//    def part = self.init
//    def succeed(nextStart: Interval[T]) = self
//    def fail(err: Throwable) = {
//      println("LoopOops: " + err)
//      err.printStackTrace(System.out)
//      self
//    }
//  }
//}

class LoopState[T](init: T) extends WaitingState[T] { self =>
  def begin = new PrepareState[T] {
      def requested = self.init
      def fail(err: Throwable) = {
        println(err)
        self
      }
      def willAccept(intr: T) = Right(new RunningState[T] {
        def succeed = self
        def fail(err: Throwable) = {
          println(err)
          self
        }
      })
    }
}

/*
 * NOT YET WORKING
 *
class WordStreamer(env: Env) extends AbstractJob(env) {
  val name = "WordStreamer"
  import WordStreamer._  // assumed to hold flatmapper and sources

  val input = "/user/tom/hqha/part-00000"
  val output = "/user/tom/testout.txt"

  implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 5L)
  val src: Producer[Scalding, String] =
    Producer.source[Scalding, String](Scalding.pipeFactoryExact[String]( _ => TextLine("pathToInput")))

  implicit var batcher = Batcher.unit

  val vbs = new VersionedBatchStore[String, Long, String, Long](output,
    3, batcher)(null)(identity)
  // val istore = VersionedBatchStore[String, Long]("myStorePath", 3)(_._2)(identity)
  val store: Scalding#Store[String,Long] = new InitialBatchedStore(batcher.currentBatch - 2L, vbs)
  val intr = Interval.leftClosedRightOpen(new Date(0L), new Date(5L))
  var ws: WaitingState[java.util.Date] = new LoopState(intr)
  val counter = wordCount[Scalding](src, store)
  val job = new Scalding("wordcount")

  println("=== Constructor done ===")

  // implicit val batcher: Batcher = Batcher.ofHours(2)
  // Now, job creation is easy!
}
 */

class WordStreamer(args : Args) extends Job(args) {

  def batchedCover(batcher: Batcher, minTime: Long, maxTime: Long): Interval[Timestamp] =
      batcher.cover(
        Interval.leftClosedRightOpen(Timestamp(minTime), Timestamp(maxTime+1L))
      ).mapNonDecreasing(b => batcher.earliestTimeOf(b.next))


  def wordCount[P <: Platform[P]](
    source: Producer[P, String],
    store: P#Store[String, Long]) =
      source
        .flatMap { line => line.split("\\s+").map(_ -> 1L) }
        .sumByKey(store)


//  override def validate: Unit = {
//    True
//  }

  def main(args: Array[String]) {
    val inout = if (args.length > 0) args(0) else "/var/summingbird/scaldingtest/obh/input.txt"
    println ("Running: " + inout)
    val output = "/var/summingbird/scaldingtest/obh/output/"
    // dummy extractor
    implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 5L)
    val src: Producer[Scalding, String] =
      Producer.source[Scalding, String](Scalding.pipeFactoryExact[String]( _ => TextLine(inout)))
    val batcher: Batcher = Batcher.ofHours(1)
    // var batcher = Batcher.unit
    val vbs = new VersionedBatchStore[String, Long, String, Long](output,
      3, batcher)(null)(identity)
    // val istore = VersionedBatchStore[String, Long]("myStorePath", 3)(_._2)(identity)
    val store: Scalding#Store[String,Long] = new InitialBatchedStore(batcher.currentBatch - 2L, vbs)
    //val intr = Interval.leftClosedRightOpen(new Date(System.currentTimeMillis() - 2*1000*60*60), new Date())
    val intr = batchedCover(batcher, 0L, 200000L)
    var ws: WaitingState[Interval[Timestamp]] = new LoopState(intr)
    val counter = wordCount[Scalding](src, store)
    val mode = Hdfs(true, new Configuration)
    val scald = Scalding("wordcount")
    ws = scald.run(ws, mode, counter)
  }

}
