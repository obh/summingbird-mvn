//import cascading.tuple.Tuple
//import com.twitter.scalding.Args
//import java.util
////import scala.Tuple
//
///**
// * Created by rohit.sharma on 10/03/14.
// */

//import com.twitter.scalding._
//
//class FriendFinderJob(args : Args) extends Job(args) {
//  val input = TextLine(args("input"))
//  input.flatMapTo('line -> 'assoc, 'list) {
//    line : (String) =>
//      val mapOut = List(("champ", "nothingishere"))
//      val friendList = line.split(" ")
//      friendList[1].split(",").foreach{ e: String => mapOut += new Tuple(user + e, friendList)}
//      mapOut
//  }.groupBy('assoc) {
//    _.size()
//  }.write(Tsv(args("output")))
//}
