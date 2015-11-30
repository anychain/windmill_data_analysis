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
val conditionStr = "c1='WT00110'"

val whereCondition = conditionStr.replaceAll(",", " and ")
val valueStr = "c2=1"
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

val fs = FileSystem.get(new Configuration())

val fsPath = "hdfs://edp01.esse.io:8020/apps/hive/warehouse/poc_merge/part-r-00000-3d6d6099-46e0-4542-823e-455df594bdff.gz.parquet"
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
                        valueMap.updated(k, v1)
                    }else{
                        valueMap.updated(k, v)
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