import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.{Producer, Platform, TimeExtractor}
import java.util.Date

object StatusStreamer {
  /**
    * These two items are required to run Summingbird in
    * batch/realtime mode, across the boundary between storm and
    * scalding jobs.
    */
  implicit val timeOf: TimeExtractor[String] = TimeExtractor(_ => new Date().getTime)
//  implicit val timeOf: TimeExtractor[String] = TimeExtractor(_ => 1)
  implicit val batcher = Batcher.ofHours(1)

  def tokenize(text: String) : TraversableOnce[String] =
    text.toLowerCase
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .split("\\s+")

  /**
    * The actual Summingbird job. Notice that the execution platform
    * "P" stays abstract. This job will work just as well in memory,
    * in Storm or in Scalding, or in any future platform supported by
    * Summingbird.
    */
  def wordCount[P <: Platform[P]](source: Producer[P, String], store: P#Store[String, Long]) =
    source
      .filter(_ != null)
      .flatMap { text: String => tokenize(text).map(_ -> 1L) }
      .sumByKey(store)
}