package tube.analysis


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
  *
  * compile group: 'org.apache.spark', name: 'spark-streaming_2.11', version: '2.2.0'
    compile group: 'org.apache.spark', name: 'spark-streaming-kafka-0-10_2.11', version: '2.2.0'
  *
  * Created by maniram on 10/2/18.
  */
object Cat_video_count {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]){

  val conf = new SparkConf().setMaster("local[3]").setAppName("Count_video_in_Category")
  val sc = new SparkContext(conf)

  val youtube_data = sc.textFile("/home/maniram/data/youtubedata.txt")

    //Mapper
    val values = youtube_data.filter(line=>line.split("\\t").length>2).map{ line =>
       val lst = line.split("\\t")
      //(video_category ,1 )
       (lst(3),1)
       }

    // Reducer
   val reducedData = values.reduceByKey(_+_).map{case(x,y)=>(y,x)}.sortByKey(false)

    // Save data into local file systrem
    reducedData .saveAsTextFile("/home/maniram/data/TubeAnalysis/Category_video_counts")

    reducedData .collect().foreach(println)
    sc.stop()

}
}