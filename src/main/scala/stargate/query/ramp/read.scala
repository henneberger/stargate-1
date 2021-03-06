package stargate.query.ramp

import java.{lang, util}
import java.util.UUID

import stargate.cassandra.CassandraKey
import stargate.model.{ScalarComparison, ScalarCondition}
import stargate.util.AsyncList
import stargate.{cassandra, query, schema}

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}

object read {

  type MaybeRead[T] = Future[Option[List[T]]]
  type MaybeReadRows = MaybeRead[Map[String,Object]]

  def filterLastValidState(context: Context, before: TransactionId, states: List[Map[String,Object]]): MaybeReadRows = {
    def firstValidState(states: List[Map[String,Object]]): MaybeReadRows = {
      states.headOption.map(head => {
        val transactionId = head(schema.TRANSACTION_ID_COLUMN_NAME).asInstanceOf[TransactionId]
        context.getState(transactionId).flatMap(status => {
          def isDeleted = head.get(schema.TRANSACTION_DELETED_COLUMN_NAME).map(_.asInstanceOf[java.lang.Boolean]).getOrElse(java.lang.Boolean.FALSE)
          if(status == TransactionState.SUCCESS) {
            Future.successful(Some(if(isDeleted) List.empty else List(head)))
          } else if(status == TransactionState.FAILED) {
            firstValidState(states.tail)
          } else {
            Future.successful(None)
          }
        })(context.executor)
      }).getOrElse(Future.successful(Some(List.empty)))
    }
    val beforeStates = states.reverse.dropWhile(row => TransactionIdOrdering.compare(row(schema.TRANSACTION_ID_COLUMN_NAME).asInstanceOf[TransactionId], before) >= 0)
    firstValidState(beforeStates)
  }

  def flatten[T](maybeReads: AsyncList[MaybeRead[T]], executor: ExecutionContext): MaybeRead[T] = {
    val unwrapped = AsyncList.unfuture(maybeReads, executor)
    val noneFailed = unwrapped.filter(_.isEmpty, executor).isEmpty(executor)
    noneFailed.flatMap(noneFailed => {
      if(noneFailed) {
        unwrapped.toList(executor).map(list => Some(list.flatMap(_.get)))(executor)
      } else {
        Future.successful(None)
      }
    })(executor)
  }

  def filterLastValidStates(context: Context, before: TransactionId, rows: AsyncList[Map[String,Object]], key: CassandraKey): MaybeReadRows = {
    val executor = context.executor
    val keyWithoutTransactionId = key.combinedMap.removed(schema.TRANSACTION_ID_COLUMN_NAME)
    def groupKey(entity: Map[String,Object]) = keyWithoutTransactionId.view.mapValues(c => entity.get(c.name).orNull).toMap
    val grouped = AsyncList.contiguousGroups(rows, groupKey, executor)
    flatten(grouped.map(filterLastValidState(context, before, _), executor), executor)
  }

  def resolveRelation(context: Context, before: TransactionId, entityName: String, fromIds: List[UUID], relationName: String): MaybeReadRows = {
    val table = context.model.relationTables((entityName, relationName))
    val conditions = List(ScalarCondition[Object](schema.RELATION_FROM_COLUMN_NAME, ScalarComparison.IN, fromIds))
    val rows = cassandra.queryAsyncMaps(context.session, query.read.selectStatement(table.keyspace, table.name, conditions).build, context.executor)
    filterLastValidStates(context, before, rows, table.columns.key)
  }
  def resolveRelationIds(context: Context, before: TransactionId, entityName: String, fromIds: List[UUID], relationName: String): MaybeRead[UUID] = {
    resolveRelation(context, before, entityName, fromIds, relationName).map(_.map(_.map(_(schema.RELATION_TO_COLUMN_NAME).asInstanceOf[UUID])))(context.executor)
  }

  def entityIdToObjectStates(context: query.Context, entityName: String, id: UUID): Future[List[Map[String,Object]]] = {
    val baseTable = context.model.baseTables(entityName)
    val select = query.read.selectStatement(baseTable, Map((schema.ENTITY_ID_COLUMN_NAME, id)))
    cassandra.queryAsyncMaps(context.session, select.build, context.executor).toList(context.executor)
  }

  def entityIdToLastValidState(context: Context, before: TransactionId, entityName: String, id: UUID): MaybeReadRows = {
    val states = entityIdToObjectStates(context.queryContext, entityName, id)
    states.flatMap(filterLastValidState(context, before, _))(context.executor)
  }
}
