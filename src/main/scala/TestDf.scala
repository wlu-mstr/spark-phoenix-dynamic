/**
  * Created by dingbingbing on 4/1/18.
  */

import com.wlu.phoenix.dynamic.toDynamicDfFunctions
import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._


object TestDf extends App {

  val hbaseConnectionString = "localhost:2181"
  val sc = new SparkContext("local", "phoenix-test")
  val sqlContext = new SQLContext(sc)

  // schema configurations
  //// (fieldname, fieldtype)
  val configSchema = List(
    ("eventid", "integer"),
    ("eventtime", "timestamp"),
    ("eventtype", "string"),
    ("name", "string"),
    ("birth", "timestamp"),
    ("age", "integer")
  )
  //// phoenix table name
  val phTable = "EVENTLOG"
  //// zookeeper config
  val zkUrl = "localhost:2181"
  //// configuration of phoenix upsert (dynamic) sql
  val sql = "UPSERT  INTO EventLog (EVENTID, EVENTTIME, EVENTTYPE, name CHAR(30), birth time, age BIGINT) VALUES (?, ?, ?, ?, ?, ?)"


  // test input json data
  val lines = List(
    """{"name":"Michael", "eventid":21, "eventtime":"2018-02-01 12:00:00", "eventtype":"tp"}""",
    """{"eventid":22,"name":"Andy", "birth":"2018-01-01 12:00:00", "age":10,"eventtime":"2018-02-01 12:00:00", "eventtype":"tp"}""",
    """{"eventid":23,"name":"Justin", "birth":"2018-01-01 13:00:00","eventtime":"2018-02-01 12:00:00", "eventtype":"tp"}"""
  )

  // create spark sql schema
  val schema = StructType(configSchema.map{
    cf => StructField(cf._1, DataType.fromJson("\"" + cf._2 + "\""))
  })

  // load json data frame
  val json_ds = sqlContext.read.schema(schema).json(sc.parallelize(lines))
  json_ds.show()


  // config of upsert sql
  val configuration = new Configuration()
  configuration.set(PhoenixConfigurationUtil.UPSERT_STATEMENT, sql)

  json_ds.saveToPhoenix(phTable, configuration,  zkUrl = Some(zkUrl))


}
