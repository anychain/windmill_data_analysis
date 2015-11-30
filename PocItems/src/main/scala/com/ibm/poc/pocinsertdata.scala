package com.ibm.poc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType,StructField,StringType,FloatType}
import org.apache.spark.sql.SaveMode
import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.sql.Timestamp
import java.sql.SQLException
import java.lang.Long
import org.apache.spark.rdd.RDD

object PocInsertData {

  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Syntax: com.ibm.poc.PocInsertData <Spark Master URL> <HDFS URL> <hive hdfs path> <table name> <csv file path in HDFS>")
      System.exit(1)
    }
    val sparkMster = args(0) // spark://bd001:7077
    val hdfs = args(1) // hdfs://bd001:8020
    val hivePath = args(2) //"/user/hive/warehouse/"
    val tableName = args(3) // poc_merge
    val srcPath = args(4)
    val conf = new SparkConf().setAppName("PocInsertData Application")
    conf.setMaster(sparkMster)
    val sc = new SparkContext(conf)
    insertData(hdfs, tableName, srcPath, hivePath, sc)
  }

  def insertData(hdfs: String, tableName: String, srcPath: String, hivePath: String, sc: SparkContext) = {
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.sql("SET hive.metastore.warehouse.dir=" + hdfs + hivePath)
    hiveContext.sql("SET hive.exec.dynamic.partition.mode=nostrick")
    hiveContext.sql("SET hive.exec.dynamic.partition=true")
    hiveContext.sql("SET hive.exec.max.dynamic.partitions=100000")
    hiveContext.setConf("spark.sql.planner.externalSort", "true")
    hiveContext.setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val opts = Map("header" -> "false", "delimiter" -> ",")

    val source = hiveContext.read.format("com.databricks.spark.csv").options(opts).load(hdfs + srcPath)
    //create data frame
    var row_rdd = PocData.createRDD(source)
    val pocDataFrame = hiveContext.createDataFrame(row_rdd, PocData.dataSchema)
    //val pocDataFrame = PocData.dataFrame(source, hiveContext)
    pocDataFrame.registerTempTable("poc_tmp_insert")
    //hiveContext.sql("insert into table " + tableName + " select * from poc_tmp_insert")
    pocDataFrame.na.drop().write.mode(SaveMode.Append).save(hdfs + hivePath + tableName)
  }

}