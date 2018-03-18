package tube.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * This program extract all tyhe category from the data on which video has bveen uploaded
  *
  *
  * Created by maniram on 10/2/18.
  */
object Extract_Video_Categories {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Extract_Video_Category")
    val sc = new SparkContext(conf)

    val raw_data = sc.textFile("/home/maniram/data/youtubedata.txt")
    val Categories = raw_data.filter(line => line.split("\\t").length > 4).map { x =>
      val cat = x.split("\\t")(3)
      (cat)
    }

    Categories.distinct().saveAsTextFile("/home/maniram/data/TubeAnalysis/Categories")
    Categories.distinct().foreach(println)
    sc.stop()
  }
  }
