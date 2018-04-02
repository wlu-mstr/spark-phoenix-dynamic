import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Wei Lu on 3/30/18.
  */


//import org.apache.phoenix.spark.toProductRDDFunctions
import com.wlu.phoenix.dynamic.toDynamicRDDFunctions

object TestSp extends App {
  val conf = new SparkConf().setMaster("local[4]").setAppName("ann")
    .set("spark.ui.enabled", "false")
  val sc = new SparkContext(conf)



  val dataSet = List((1017, "2018-01-01 12:00:00", "t1",1, 1024),
    (1027, "2018-01-01 12:00:00", "t2", 2, 2048),
    (1037, "2018-01-01 12:00:00", "t3", 3, 4096))

  val configuration = new Configuration()

  val sql = "UPSERT  INTO EventLog (EVENTID, EVENTTIME, EVENTTYPE, maxMemory BIGINT, usedMemory BIGINT) VALUES (?, ?, ?, ?, ?)"
  val fieldN = 5

  configuration.set(PhoenixConfigurationUtil.UPSERT_STATEMENT, sql)

  val pd = sc.parallelize(dataSet)


  pd.saveToPhoenix(
      "EventLog",
      fieldN,
      configuration,
      zkUrl = Some("localhost:2181")

    )

}
