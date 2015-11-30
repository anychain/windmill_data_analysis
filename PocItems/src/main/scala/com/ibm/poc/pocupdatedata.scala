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


object PocUpdateData {
  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println("Syntax: com.ibm.poc.PocUpdateData <Spark Master URL> <HDFS URL> <hive hdfs path> <table name> <conditions k1='v1',k2='v2',...> <values c1=v1,c2=v2,...>")
      System.exit(1)
    }
    val sparkMster = args(0) // spark://bd001:7077
    val hdfs = args(1) // hdfs://bd001:8020
    val hivePath = args(2) //"/user/hive/warehouse"
    val tableName = args(3)
    val conditionStr = args(4)
    val valueStr = args(5)
    val conf = new SparkConf().setAppName("PocUpdateData Application")
    conf.setMaster(sparkMster)
    val sc = new SparkContext(conf)
    updateData(hdfs, tableName, conditionStr, valueStr, hivePath, sc)
  }

  def updateData(hdfs: String, tableName: String, conditionStr: String, valueStr: String, hivePath: String, sc: SparkContext) = {
    val vsMap = valueStr.split(",").map(_.split("=")).map{ case Array(k,v) => (k,v) }.toMap
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.sql("SET hive.metastore.warehouse.dir=" + hdfs + hivePath)
    hiveContext.sql("SET hive.exec.dynamic.partition.mode=nostrick")
    hiveContext.sql("SET hive.exec.dynamic.partition=true")
    hiveContext.sql("SET hive.exec.max.dynamic.partitions=100000")
    hiveContext.setConf("spark.sql.planner.externalSort", "true")
    hiveContext.setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val opts = Map("header" -> "false", "delimiter" -> ",")

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
    //stRdd.foreach{
    fileNameList.foreach{
        item => {
            val fsPath = item
            println("---------------> " +fsPath)
            val opts = Map("header" -> "false", "delimiter" -> ",")
            val df = hiveContext.read.format("parquet").options(opts).load(fsPath)
            var row_rdd = PocData.createRDD(df)
            val pocDataFrame = hiveContext.createDataFrame(row_rdd, PocData.dataSchema)
            val tmpTableName = "poc_tmp_update" + accum
            pocDataFrame.registerTempTable(tmpTableName)
            val res = hiveContext.sql("select * from " + tmpTableName + " where " + whereCondition)
            if (res.count > 0) {
                val oldData = row_rdd.subtract(res.toJavaRDD)
                //merge res with updated values
                var updatedList = List[Map[String,Nothing]]()
                res.collect().foreach{
                    row => {
                        val schema = PocData.dataSchema
                        val valueMap = row.getValuesMap(schema.fieldNames)
                        vsMap.keys.foreach{
                            k => {
                                val f = schema.apply(k)
                                val v = vsMap(k)
                                if (f.dataType == FloatType){
                                    val v1 = v.toString().toFloat
                                    valueMap.updated(k.toLowerCase, v1)
                                }else{
                                    valueMap.updated(k.toLowerCase, v)
                                }
                            }
                        }
                        updatedList = valueMap +: updatedList
                    }
                }
                val updatedRdd = PocData.createRDDFromMap(sc, updatedList)
                val newData = oldData ++ updatedRdd
                //then join the subtracted the data set
                val newDataFrame = hiveContext.createDataFrame(newData, PocData.dataSchema)
                println("saving -------------> " + fsPath)
                newDataFrame.na.drop().write.mode(SaveMode.Append).save(hdfs + hivePath + tableName)
                //newDataFrame.write.format("parquet").save(fsPath)
                println("deleting ------------------> " + fsPath)
                fs.delete(new Path(fsPath), true)
            }
            accum += 1
            
        }
    }
  }

}