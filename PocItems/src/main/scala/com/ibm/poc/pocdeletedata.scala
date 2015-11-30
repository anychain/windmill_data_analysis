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
import org.apache.hadoop.io._;
import org.apache.hadoop.fs._;
import org.apache.hadoop.conf._;


object PocDeleteData {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Syntax: com.ibm.poc.PocDeleteData <Spark Master URL> <HDFS URL> <hive hdfs path> <table name> <conditions k1='v1',k2='v2',...>")
      System.exit(1)
    }
    val sparkMster = args(0) // spark://bd001:7077
    val hdfs = args(1) // hdfs://bd001:8020
    val hivePath = args(2) //"/user/hive/warehouse"
    val tableName = args(3)
    val conditionStr = args(4)
    val conf = new SparkConf().setAppName("PocDeleteData Application")
    conf.setMaster(sparkMster)
    val sc = new SparkContext(conf)
    deleteData(hdfs, tableName, conditionStr, hivePath, sc)
  }

  def deleteData(hdfs: String, tableName: String, conditionStr: String, hivePath: String, sc: SparkContext) = {
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.sql("SET hive.metastore.warehouse.dir=" + hdfs + hivePath)
    hiveContext.sql("SET hive.exec.dynamic.partition.mode=nostrick")
    hiveContext.sql("SET hive.exec.dynamic.partition=true")
    hiveContext.sql("SET hive.exec.max.dynamic.partitions=100000")
    hiveContext.setConf("spark.sql.planner.externalSort", "true")
    hiveContext.setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val accum = sc.accumulator(0, "My Accumulator")
    val whereCondition = conditionStr.replaceAll(",", " and ")
    
    val fs = FileSystem.get(new Configuration())
    val status = fs.listStatus(new Path(hdfs + hivePath + tableName))
    var fileNameList = List[String]()
    status.foreach{
        i => {
            if (!i.getPath().getName().startsWith("_"))
                fileNameList = i.getPath().toString() +: fileNameList
        }
    }
    //val stRdd = sc.parallelize(fileNameList)
    fileNameList.foreach{
        item => {
            val fsPath = item
            println("---------------> " +fsPath)
            val opts = Map("header" -> "false", "delimiter" -> ",")
            val df = hiveContext.read.format("parquet").options(opts).load(fsPath)
            var row_rdd = PocData.createRDD(df)
            val pocDataFrame = hiveContext.createDataFrame(row_rdd, PocData.dataSchema)
            val tmpTableName = "poc_tmp_delete" + accum
            pocDataFrame.registerTempTable(tmpTableName)
            val res = hiveContext.sql("select * from " + tmpTableName + " where " + whereCondition)
            if (res.count > 0) {
                //hiveContext.refreshTable(tableName)
                val newData = row_rdd.subtract(res.toJavaRDD)
                val newDataFrame = hiveContext.createDataFrame(newData, PocData.dataSchema)
                val tmpNewTN = "poc_tmp_delete_new" + accum
                newDataFrame.registerTempTable(tmpNewTN)
                val res1 = hiveContext.sql("select * from " + tmpNewTN)
                if (res1.count > 0){
                    println("saving -------------> " + fsPath)
                    newDataFrame.na.drop().write.mode(SaveMode.Append).save(hdfs + hivePath + tableName)
                }else{
                    println(" -----------------> no data in " + fsPath)
                }
                println("deleting ------------------> " + fsPath)
                fs.delete(new Path(fsPath), true)
            }
            accum += 1
        }
    }
  }

}