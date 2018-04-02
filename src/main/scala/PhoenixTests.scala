import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.mapreduce.util.ConnectionUtil
import org.apache.phoenix.schema.PTable
import org.apache.phoenix.util.{PhoenixRuntime, SchemaUtil}

/**
  * Created by dingbingbing on 4/1/18.
  */
object PhoenixTests extends App {

  val conf = new Configuration()
  conf.set("phoneix.mapreduce.output.cluster.quorum", "localhost:2181")

  val conn = ConnectionUtil.getOutputConnection(conf)

  val table: PTable = PhoenixRuntime.getTable(conn, SchemaUtil.normalizeFullTableName("EventLog"))

  val ac = table.getColumns.get(2)

  println(ac.getName)
}
