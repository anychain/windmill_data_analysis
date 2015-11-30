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

import com.ibm.poc.PocData


val hdfs = "hdfs://edp01.esse.io:8020"
val hivePath = "/apps/hive/warehouse/"
val tableName = "poc_merge"
val conditionStr = "c1='WT00116'"
val whereCondition = conditionStr.replaceAll(",", " and ")

val hiveContext = new HiveContext(sc)
import hiveContext.implicits._
hiveContext.sql("SET hive.metastore.warehouse.dir=" + hdfs + hivePath)
hiveContext.sql("SET hive.exec.dynamic.partition.mode=nostrick")
hiveContext.sql("SET hive.exec.dynamic.partition=true")
hiveContext.sql("SET hive.exec.max.dynamic.partitions=100000")
hiveContext.setConf("spark.sql.parquet.binaryAsString","true")
hiveContext.setConf("spark.sql.parquet.mergeSchema", "true")
val accum = sc.accumulator(0, "My Accumulator")


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
        val disres = hiveContext.sql("select distinct(c1) from " + tmpTableName)
        disres.show
        val disres1 = hiveContext.sql("select count(*) from " + tmpTableName + " where " + whereCondition)
        disres1.show
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


val fsPath = "hdfs://edp01.esse.io:8020/apps/hive/warehouse/poc_merge/part-r-00000-525886a4-284f-4f63-a379-a742736bd23d.gz.parquet"
println("---------------> " +fsPath)
val opts = Map("header" -> "false", "delimiter" -> ",")
val df = hiveContext.read.format("parquet").options(opts).load(fsPath)
var row_rdd = PocData.createRDD(df)
val pocDataFrame = hiveContext.createDataFrame(row_rdd, PocData.dataSchema)
val tmpTableName = "poc_tmp_delete" + accum
pocDataFrame.registerTempTable(tmpTableName)
val res = hiveContext.sql("select * from " + tmpTableName + " where " + whereCondition)
if (res.count > 0) {
    println("deleting ------------------> " + fsPath)
    fs.delete(new Path(fsPath), true)
    val newData = row_rdd.subtract(res.toJavaRDD)
    val newDataFrame = hiveContext.createDataFrame(newData, PocData.dataSchema)
    val tmpNewTN = "poc_tmp_delete_new" + accum
    newDataFrame.registerTempTable(tmpNewTN)
    val res1 = hiveContext.sql("select * from " + tmpNewTN)
    if (res1.count > 0){
        res1.show()
        println("saving -------------> " + fsPath)
        newDataFrame.na.drop().write.mode(SaveMode.Append).save(hdfs + hivePath + tableName)
    }else{
        println(" -----------------> no data in " + fsPath)
    }
}
accum += 1