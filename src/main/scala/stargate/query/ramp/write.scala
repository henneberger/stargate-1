package stargate.query.ramp

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.internal.core.util.Strings
import stargate.cassandra
import stargate.cassandra.CassandraTable
import stargate.query
import stargate.schema.{TRANSACTION_DELETED_COLUMN_NAME, TRANSACTION_ID_COLUMN_NAME}

import scala.concurrent.{ExecutionContext, Future}

object write {


  type RampRows = List[(CassandraTable, Map[String,Object])]
  case class WriteResult(success: Boolean, writes: RampRows, cleanup: RampRows)



  def compareAndSet(table: CassandraTable, write: Map[String,Object], previous: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[WriteResult] = {
    implicit val ec: ExecutionContext = executor
    for {
      _ <- cassandra.executeAsync(session, query.write.insertStatement(table, write).build, executor)
      rows <- cassandra.queryAsync(session, query.read.selectStatement(table, previous.removed(TRANSACTION_ID_COLUMN_NAME)).build, executor).toList(executor)
      latestTransactions = rows.takeRight(2).map(_.getUuid(Strings.doubleQuote(TRANSACTION_ID_COLUMN_NAME)))
      success = latestTransactions == List(previous(TRANSACTION_ID_COLUMN_NAME), write(TRANSACTION_ID_COLUMN_NAME))
      writes = List((table, write))
      deleteCleanup = if(write.get(TRANSACTION_DELETED_COLUMN_NAME).contains(java.lang.Boolean.TRUE:Object)) List((table, write)) else List.empty
      cleanup = List((table, previous)) ++ deleteCleanup
    } yield WriteResult(success, writes, cleanup)
  }

}
