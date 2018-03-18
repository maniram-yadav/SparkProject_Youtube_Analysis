package tube.analysis

import com.sun.media.jfxmedia.locator.LocatorCache.CacheReference
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

/**
  * Created by maniram on 18/3/18.
  *
  * Select the users who publishes the video only on single category and also on multiple category
  *
  *
  */
object User_singleCategory {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("user video in single category")
    val sc = new SparkContext(conf)
    val raw_data = sc.textFile("/home/maniram/data/youtubedata.txt")

    val userand_Cateory = raw_data.filter(line => line.split("\\t").length > 3).map { x =>
      val video_info = x.split("\\t")
      (video_info(1),video_info(3))
    }
    var category_lst = List[Any]()
    var category_set = Set[Any]()


  val user_category_grp = userand_Cateory.groupByKey()

    val user_category_detail = user_category_grp.map{ case(userId,category_Buffer) =>

      category_lst = Nil
      category_Buffer.foreach{ category =>      // Extracting user category into a list from compactbuffer
      category_lst = category :: category_lst     // appned category into the list
    }

      category_set = category_lst.toSet // get unique category from list apllying set
      //( userid, user video category list, category list length,user uploaded video only on  single category)
      (userId,category_set,category_set.size,if(category_set.size==1) true else false)

    }

    // Use Rdd uploaded video only on single category
    val user_upload_onecategory = user_category_detail.filter( record => record._4==true)
    // Use Rdd uploaded video on multiple category
    val user_upload_mulcategory = user_category_detail.filter(record => record._4==false)

    println("Single Category User")
    user_upload_onecategory.foreach(println)

    println("Multiple Category User")
    user_upload_mulcategory.foreach(println)

    //user_category_detail.foreach(println)
    sc.stop()

}
}
