package tube.analysis
import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
  *
  *  This program finds the number of video uploaded by a user
  *
  *
  * Created by maniram on 18/3/18.
  */
object Uploader_Video_Count {


  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {
  val conf = new SparkConf().setMaster("local[2]").setAppName("Number of Uploaded Video by User")
  val sc = new SparkContext(conf)

  val raw_data = sc.textFile("/home/maniram/data/youtubedata.txt")

  // map the user
  val user_list = raw_data.filter(line => line.split("\\t").length >1).map(line => (line.split("\\t")(1),1) )
  //reduce rdd by the user video uploaded
   val video_count_ofuser = user_list.reduceByKey(_+_).sortBy(_._2,ascending = false)

    video_count_ofuser.saveAsTextFile("/home/maniram/data/TubeAnalysis/User_Video_Count")
  video_count_ofuser.foreach(println)
    sc.stop()
}
}
