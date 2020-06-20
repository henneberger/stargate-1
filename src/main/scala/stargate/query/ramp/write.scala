package stargate.query.ramp

import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.internal.core.util.Strings
import stargate.{cassandra, query, schema}
import stargate.cassandra.CassandraTable
import stargate.model.OutputModel
import stargate.schema.{TRANSACTION_DELETED_COLUMN_NAME, TRANSACTION_ID_COLUMN_NAME}

import scala.concurrent.{ExecutionContext, Future}

object write {

  sealed trait WriteOp
  case class InsertOp(table: CassandraTable, data: Map[String,Object]) extends WriteOp
  case class CompareAndSetOp(table: CassandraTable, data: Map[String,Object], previous: Map[String,Object]) extends WriteOp

  type RampRows = List[(CassandraTable, Map[String,Object])]
  case class WriteResult(success: Boolean, writes: RampRows, cleanup: RampRows)



  def createEntity(tables: List[CassandraTable], payload: Map[String,Object]): (UUID, List[InsertOp]) = {
    val uuid = UUID.randomUUID()
    val idPayload = payload.updated(schema.ENTITY_ID_COLUMN_NAME, uuid)
    (uuid, tables.map(t => InsertOp(t, idPayload)))
  }
  def updateEntity(tables: List[CassandraTable], currentEntity: Map[String,Object], changes: Map[String,Object]): List[WriteOp] = {
    tables.flatMap(table => {
      val keyChanged = table.columns.key.combined.exists(col => changes.get(col.name).orNull != null)
      if(keyChanged) {
        val delete = CompareAndSetOp(table, currentEntity.updated(schema.TRANSACTION_DELETED_COLUMN_NAME, java.lang.Boolean.TRUE), currentEntity)
        val insert = InsertOp(table, currentEntity++changes)
        List(delete, insert)
      } else {
        List(CompareAndSetOp(table, currentEntity ++ changes, currentEntity))
      }
    })
  }
  def deleteEntity(tables: List[CassandraTable], currentEntity: Map[String,Object]): List[WriteOp] = {
    tables.map(table => CompareAndSetOp(table, currentEntity.updated(schema.TRANSACTION_DELETED_COLUMN_NAME, java.lang.Boolean.TRUE), currentEntity))
  }

  def compareAndSet(table: CassandraTable, write: Map[String,Object], previous: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[WriteResult] = {
    implicit val ec: ExecutionContext = executor
    for {
      _ <- cassandra.executeAsync(session, query.write.insertStatement(table, write).build, executor)
      rows <- cassandra.queryAsync(session, query.read.selectKeysStatement(table, previous.removed(TRANSACTION_ID_COLUMN_NAME)).build, executor).toList(executor)
      latestTransactions = rows.takeRight(2).map(_.getUuid(Strings.doubleQuote(TRANSACTION_ID_COLUMN_NAME)))
      success = latestTransactions == List(previous(TRANSACTION_ID_COLUMN_NAME), write(TRANSACTION_ID_COLUMN_NAME))
      writes = List((table, write))
      deleteCleanup = if(write.get(TRANSACTION_DELETED_COLUMN_NAME).contains(java.lang.Boolean.TRUE:Object)) List((table, write)) else List.empty
      cleanup = List((table, previous)) ++ deleteCleanup
    } yield WriteResult(success, writes, cleanup)
  }

  def insert(table: CassandraTable, write: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[WriteResult] = {
    cassandra.executeAsync(session, query.write.insertStatement(table, write).build, executor).map(_ => WriteResult(true, List((table, write)), List.empty))(executor)
  }

  def execute(context: Context, op: WriteOp): Future[WriteResult] = {
    op match {
      case InsertOp(table, data) => insert(table, data, context.session, context.executor)
      case CompareAndSetOp(table, data, previous) => compareAndSet(table, data, previous, context.session, context.executor)
    }
  }


  def relationColumnValues(from: UUID, to: UUID): Map[String, UUID] = Map((schema.RELATION_FROM_COLUMN_NAME, from),(schema.RELATION_TO_COLUMN_NAME, to))
  def updateBidirectionalRelation(statement: (CassandraTable,UUID,Map[String,Object]) => WriteOp, model: OutputModel, transactionId: UUID, fromEntity: String, fromRelationName: String, ids: Map[String,Object]): List[WriteOp] = {
    val fromRelation =  model.input.entities(fromEntity).relations(fromRelationName)
    val toEntity = fromRelation.targetEntityName
    val toRelationName = fromRelation.inverseName
    val fromTable = model.relationTables((fromEntity, fromRelationName))
    val toTable = model.relationTables((toEntity, toRelationName))
    val inverseIds = ids.updated(schema.RELATION_FROM_COLUMN_NAME, ids(schema.RELATION_TO_COLUMN_NAME)).updated(schema.RELATION_TO_COLUMN_NAME, ids(schema.RELATION_FROM_COLUMN_NAME))
    List(statement(fromTable, transactionId, ids), statement(toTable, transactionId, inverseIds))
  }
  def createBidirectionalRelation(model: OutputModel, transactionId: UUID, fromEntity: String, fromRelationName: String, fromId: UUID, toId: UUID): List[WriteOp] = {
    def create(table: CassandraTable, transactionId: UUID, ids: Map[String,Object]): InsertOp = {
      InsertOp(table, ids.updated(schema.TRANSACTION_ID_COLUMN_NAME, transactionId))
    }
    updateBidirectionalRelation(create, model, transactionId, fromEntity, fromRelationName, relationColumnValues(fromId, toId))
  }
  def deleteBidirectionalRelation(model: OutputModel, transactionId: UUID, fromEntity: String, fromRelationName: String, row: Map[String,Object]): List[WriteOp] = {
    def delete(table: CassandraTable, transactionId: UUID, ids: Map[String,Object]): CompareAndSetOp = {
      val deleteRow = ids.updated(schema.TRANSACTION_ID_COLUMN_NAME, transactionId).updated(schema.TRANSACTION_DELETED_COLUMN_NAME, java.lang.Boolean.TRUE)
      CompareAndSetOp(table, deleteRow, ids)
    }
    updateBidirectionalRelation(delete, model, transactionId, fromEntity, fromRelationName, row)
  }

}
