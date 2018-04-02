package com.wlu.phoenix.dynamic

import java.sql.SQLException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.phoenix.mapreduce.PhoenixOutputFormat
import org.apache.phoenix.mapreduce.util.{ColumnInfoToStringEncoderDecoder, PhoenixConfigurationUtil}
import org.apache.phoenix.spark.{ConfigurationUtil, PhoenixRecordWritable}
import org.apache.phoenix.util.ColumnInfo
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._


/**
  * Created by Wei Lu on 3/31/18.
  */
class DynamicColumnsRDDFunctions[A <: Product](data: RDD[A]) extends Logging with Serializable {
  def saveToPhoenix(tableName: String, n: Int,
                    conf: Configuration = new Configuration, zkUrl: Option[String] = None)
  : Unit = {

    val cols = Seq()
    // Create a configuration object to use for saving
    @transient val outConfig = ConfigurationUtil.getOutputConfiguration(tableName, cols, zkUrl, Some(conf))

    // Retrieve the zookeeper URL
    val zkUrlFinal = ConfigurationUtil.getZookeeperURL(outConfig)

    // Map the row objects into PhoenixRecordWritable
    val phxRDD = data.mapPartitions { rows =>

      // Create a within-partition config to retrieve the ColumnInfo list
      @transient val partitionConfig = ConfigurationUtil.getOutputConfiguration(tableName, cols, zkUrlFinal)
      val columns1 = PhoenixConfigurationUtil.getUpsertColumnMetadataList(partitionConfig)

      // hack here: add dynamic columns
      if (columns1.length < n) {
        Range(start = columns1.length, n).foreach {
          i => columns1.add(new ColumnInfo("xxx" + i, 1))
        }
      }
      @transient val columns = PhoenixConfigurationUtil.getUpsertColumnMetadataList(partitionConfig).toList

      ColumnInfoToStringEncoderDecoder.encode(partitionConfig, columns)


      rows.map { row =>
        val rec = new PhoenixRecordWritable(columns1.toList)
        row.productIterator.foreach { e => rec.add(e) }
        (null, rec)
      }
    }

    // Save it
    phxRDD.saveAsNewAPIHadoopFile(
      "",
      classOf[NullWritable],
      classOf[PhoenixRecordWritable],
      classOf[PhoenixOutputFormat[PhoenixRecordWritable]],
      outConfig
    )
  }
}
