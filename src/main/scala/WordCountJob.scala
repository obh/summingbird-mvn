/**
 * Created by rohit.sharma on 06/03/14.
 */
import com.twitter.scalding._


/*
 * run job as hadoop jar maven-scala-1.0-SNAPSHOT-jar-with-dependencies.jar WordCountJob --hdfs --input /tmp/pg1140.txt --output /tmp/count
 */
class WordCountJob(args : Args) extends Job(args) {
  TextLine(args("input"))
    .flatMap('line -> 'word) { line : String => line.split("""\s+""") }
    .groupBy('word) { _.size }
    .write(Tsv(args("output")))
}

