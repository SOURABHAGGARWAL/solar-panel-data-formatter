package com.renew_power.solar_panel_data_formatter.context

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

/**
 * Class used to create the spark session and spark config
 * @created By Sourabh Aggarwal on 30-MARCH-2020.
 */
class SparkContext {
  
  //Create conf object
    lazy val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark application under renew power")
    .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .set("spark.sql.parquet.cacheMetadata","false")
    .set("spark.serializer", classOf[KryoSerializer].getName)
    .set("spark.sql.tungsten.enabled", "true")
    .set("spark.eventLog.enabled", "true")
    .set("spark.io.compression.codec", "snappy")
    .set("spark.rdd.compress", "true")

    var sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  
}