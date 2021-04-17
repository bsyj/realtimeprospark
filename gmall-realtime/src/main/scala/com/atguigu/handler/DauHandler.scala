package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  def saveMidToRedis(filterByMidDStream: DStream[StartUpLog]) = {
    filterByMidDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val jedisClient: Jedis = RedisUtil.getJedisClient
        partition.foreach(log => {
          jedisClient.sadd("DAU:" + log.logDate,log.mid)
        })
        jedisClient.close()
      })
    })
  }


  def filterByGroup(filterByRedisDStream:DStream[StartUpLog]) = {

    val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.mapPartitions(partition => {
      partition.map(log => {
        ((log.mid, log.logDate), log)
      })
    })

    val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDStream.groupByKey()

    val sortWithTsDstream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })
    val value: DStream[StartUpLog] = sortWithTsDstream.flatMap(_._2)

    value

  }

  /**
   * 批次间去重
   * @param startUpLogDStream
   * @param sc
   * @return
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc:SparkContext) = {

    //    val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      val boolean: lang.Boolean = jedisClient.sismember("DAU:" + log.logDate, log.mid)
    //
    //      jedisClient.close()
    //
    //      !boolean
    //    })
    //
    //    value

    //方案二
//    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
//      val jedisClient: Jedis = RedisUtil.getJedisClient
//      val logs: Iterator[StartUpLog] = partition.filter(log => {
//        val redisKey: String = "DAU:" + log.logDate
//        val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
//        !boolean
//      })
//      jedisClient.close()
//      logs
//    })
//    value
     //方案三
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //在每个批次中获取一次连接
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1. 获取一次redis连接
      val client: Jedis = RedisUtil.getJedisClient
      val redisKey: String = "DAU:" + simpleDateFormat.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = client.smembers(redisKey)

      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      val rddBC : RDD[StartUpLog] = rdd.filter(log => { //执行在executor
        val boolean: lang.Boolean = midsBC.value.contains(log.mid)
        !boolean
      })
      client.close()
      rddBC
    })
    value
  }
}


