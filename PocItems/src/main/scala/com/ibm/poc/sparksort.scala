package com.ibm.poc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType,StructField,StringType,FloatType}
import org.apache.spark.sql.SaveMode

object SparkSort {
    if (args.length != 6) {
        System.err.println("Syntax: com.ibm.poc.SparkSort <Spark Master URL> <HDFS URL> <source path> <output path> <TAGx> <n>")
        System.exit(1)
    }
    val sparkMster = args(0) // spark://bd001:7077
    val hdfs = args(1) // hdfs://bd001:8020
    val srcPath = args(2)
    val outputPath = args(3)
    val tagx = args(4).toInt()
    val n = args(5).toInt()
    
    val conf = new SparkConf().setAppName("PocUpdateData Application")
    conf.setMaster(sparkMster)
    val sc = new SparkContext(conf)
} 