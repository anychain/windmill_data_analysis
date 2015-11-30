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
val hdfs = "hdfs://nd3.esse.io:8020"
val hiveContext = new HiveContext(sc)
import hiveContext.implicits._
hiveContext.sql("SET hive.metastore.warehouse.dir=" + hdfs + "/user/hive/warehouse")
hiveContext.sql("SET hive.exec.dynamic.partition.mode=nostrick")
hiveContext.sql("SET hive.exec.dynamic.partition=true")
hiveContext.sql("SET hive.exec.max.dynamic.partitions=100000")
hiveContext.setConf("spark.sql.planner.externalSort", "true")
hiveContext.setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

//Order
val result = hiveContext.sql("select id, tag150 from TABLE_X").sort($"tag150".desc).cache()
//rdd.glom
val withIndex = result.rdd.zipWithIndex
val indexKey = withIndex.map{case (k,v) => (v,k)}
val output = indexKey.filter{
    case (index, row) => {
      1000000 % (index + 1) == 1
    }
}
output.saveTextFile(hdfs + "/test/1")

val result = hiveContext.sql("SELECT N.ID FROM ( SELECT A.ID, A.TAG1, B.TAG2 "
 + "From testdata_parquet_hour1 AS A, testdata_parquet_hour1 AS B "
 + "WHERE to_date(A.time) > to_date(\"2015-04-06\") AND to_date(A.time)< to_date(\"2015-04-07\") "
 + "AND to_date(B.time)>to_date(\"2015-04-06\") AND to_date(B.time)<to_date(\"2015-04-07\") "
 + "JOIN ON A.ID = B.ID) AS N "
 + "WHERE N.TAG1 >1 AND N.TAG2 >3 ")



hiveContext.sql("SELECT A.ID, A.TAG1, B.TAG2 "
 + "From testdata_parquet_hour1 AS A, testdata_parquet_hour1 AS B "
 + "WHERE to_date(A.time) > to_date(\"2015-04-06\") AND to_date(A.time)< to_date(\"2015-04-07\") "
 + "AND to_date(B.time)>to_date(\"2015-04-06\") AND to_date(B.time)<to_date(\"2015-04-07\") "
 + "JOIN B ON A.ID = B.ID")