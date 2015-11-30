package io.esse.poc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType,StructField,StringType,FloatType};
import org.apache.spark.sql.SaveMode
import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.sql.Timestamp
import java.sql.SQLException
import java.lang.Long
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Merge_Parquet {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Syntax: Merge_Parquet <Spark Master URL> <HDFS URL> <Source location> <Target location> <Parquet file number>")
      System.exit(1)
    }
    val hdfs = args(1) // hdfs://bd001:8020

    val conf = new SparkConf().setAppName("POC_Load_CSV Application")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    merge_parquet(hdfs, args(2), args(3), args(4).toInt, sc)
  }

  def merge_parquet(hdfs: String, dir: String, dist: String, par_num: Integer, sc: SparkContext) = {
    val sqlContext = new SQLContext(sc)
    val size = 128 * 1024 * 1024

    sqlContext.sparkContext.hadoopConfiguration.setInt("dfs.blocksize", size)
    sqlContext.sparkContext.hadoopConfiguration.setInt("parquet.block.size", size)

    val source = sqlContext.read.load(hdfs + dir)

    source.persist(StorageLevel.MEMORY_ONLY_SER)

    source.coalesce(par_num).write.mode(SaveMode.Overwrite).save(hdfs + dist)

  }
}