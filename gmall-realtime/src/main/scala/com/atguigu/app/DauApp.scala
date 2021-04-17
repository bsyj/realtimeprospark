package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime}
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {

    //1.
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    //
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //获取kafkaStream
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaStream.mapPartitions(
      partition => {
        partition.map(
          record => {
            val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
            val date = new Date(startUpLog.ts)
            val str: String = simpleDateFormat.format(date)
            startUpLog.logDate = str.split(" ")(0)
            startUpLog.logHour = str.split(" ")(1)
            startUpLog
          }
        )
      }
    )

    //批次间去重

    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    filterByRedisDStream.cache()

    startUpLogDStream.count().print()
    filterByRedisDStream.count().print()

    //批次内去重
    val filterByMidDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)

    //经过批次内去重的数据条数
    filterByMidDStream.cache()
    filterByMidDStream.count().print()


    DauHandler.saveMidToRedis(filterByMidDStream)

    //存进Hbase

    filterByMidDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })



    ssc.start()
    ssc.awaitTermination()

  }
}
