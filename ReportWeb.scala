package com.elianda

import com.elianda.common.{AppContext, SparkBuilder}
import com.elianda.netty.NettyServer
import com.elianda.util.QueryCommonUtil
import org.apache.spark
import com.elianda.util.BHUDFFunc._
import org.slf4j.LoggerFactory

/**
  * 渤海信贷报表跑批入口
  */

class ReportApp {

}

object ReportApp {

  val logger = LoggerFactory.getLogger(classOf[ReportApp])

  def main(args: Array[String]): Unit = {
    System.setProperty("RT_REPORT_HOME", "")
    logger.info("初始化服务...")
    //初始化sparkSession
   val spark =  SparkBuilder.getSparkSession()

    AppContext.loadConf()
//    spark.udf.register("substr_duf",substr_duf)
//    val userData = Array(("a", "西安", 1, 10, 88.6), ("b", "武汉", 0, 6, 99.5), ("c", "成都", 1, 8, 100.00), ("d", "郑州", 0, 26, 49.0))
//
//    val userDF = spark.createDataFrame(userData).toDF("name", "addr", "sex", "age", "score")
//    userDF.select("substr_duf('addr',0,1)").show()

    //初始化netty服务
    new NettyServer().start()


    /* println("导入")
     val conf=ConfigFactory.load("app.conf").getConfig("orders")*/
  }

//  val m = Map("start_date" -> "20171120", "end_date" -> "20171121")
}
