package com.wlu.phoenix.dynamic

/**
  * Created by Wei Lu on 4/1/18.
  */


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.io.NullWritable
import org.apache.phoenix.mapreduce.PhoenixOutputFormat
import org.apache.phoenix.mapreduce.util.{ConnectionUtil, PhoenixConfigurationUtil}
import org.apache.phoenix.schema.PTable
import org.apache.phoenix.spark.{ConfigurationUtil, PhoenixRecordWritable}
import org.apache.phoenix.util.{ColumnInfo, PhoenixRuntime, SchemaUtil}
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._

class DynamicColumnsDataFrameFunctions(data: DataFrame) extends Logging with Serializable {

  def getTableColumnNames(tableName: String, conf: Configuration, url: String) = {
    conf.set(HConstants.ZOOKEEPER_QUORUM, url )

    val conn = ConnectionUtil.getOutputConnection(conf)
    val table: PTable = PhoenixRuntime.getTable(conn, SchemaUtil.normalizeFullTableName(tableName))
    table.getColumns.map(col => col.getName.getString).toArray
  }

  def saveToPhoenix(tableName: String, conf: Configuration = new Configuration,
                    zkUrl: Option[String] = None): Unit = {

    // Retrieve the schema field names and normalize to Phoenix, need to do this outside of mapPartitions
    val fieldArray: Array[String] = data.schema.fieldNames.map(x => SchemaUtil.normalizeIdentifier(x))
    // table fields that are in input fields
    val tableFieldArray: Array[String] = getTableColumnNames(tableName, conf, zkUrl.get)
    val inputFieldsInTable = tableFieldArray.filter(tf => fieldArray.contains(tf))
    // input fields that are not in table
    val inputFieldsNotInTable = fieldArray.filter(f => !tableFieldArray.contains(f))


    // Create a configuration object to use for `PhoenixWriter` saving
    @transient val outConfig = ConfigurationUtil.getOutputConfiguration(tableName, fieldArray, zkUrl, Some(conf))

    // Retrieve the zookeeper URL
    val zkUrlFinal = ConfigurationUtil.getZookeeperURL(outConfig)

    // Map the row objects into PhoenixRecordWritable
    val phxRDD = data.mapPartitions { rows =>

      // Create a within-partition config to retrieve the ColumnInfo list
      @transient val partitionConfig = ConfigurationUtil.getOutputConfiguration(tableName, inputFieldsInTable, zkUrlFinal)

      val columns = PhoenixConfigurationUtil.getUpsertColumnMetadataList(partitionConfig)
      // add column information for dynamic columns, hacky codes.
      inputFieldsNotInTable.foreach(_ => columns.add(new ColumnInfo("dummy_cols", 1)))

      println(columns)
      //@transient val columns = PhoenixConfigurationUtil.getUpsertColumnMetadataList(partitionConfig).toList

      rows.map { row =>
        val rec = new PhoenixRecordWritable(columns.toList)
        row.toSeq.foreach { e => rec.add(e) }
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
