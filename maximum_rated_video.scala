package tube.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *  This program finds the top  10 rated video
  *
  *
  * Created by maniram on 10/2/18.
  */
object maximum_rated_video {


  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Top_Rated_Video")
    val sc = new SparkContext(conf)

    val youtube_raw_data = sc.textFile("/home/maniram/data/youtubedata.txt")

    // Mapper
    val values = youtube_raw_data.filter(line => line.split("\\t").length > 7).map { line =>
      val lst = line.split("\\t")
      (lst(0), lst(6).toFloat,lst(3))
    }

    // New mapper after exchanging the keys
    val ratingAsKey=values.map{case(x,y,z)=>(y,x,z)}

    // Reducer and sorting
    val top10 = ratingAsKey.sortBy(_._1,ascending = false).take(10)

    // Conversion into RDD data
    val top10Rdd = sc.parallelize(top10,1)
    top10Rdd.foreach(println)

    // Saving into local file
     top10Rdd.saveAsTextFile("/home/maniram/data/TubeAnalysis/topRatedVideos")

sc.stop()
  }
}