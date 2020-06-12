package stargate.query.ramp

import java.util.UUID

import stargate.{cassandra, query, schema}

import scala.concurrent.Future

object read {

  def entityIdToObjectStates(context: query.Context, entityName: String, id: UUID): Future[List[Map[String,Object]]] = {
    val baseTable = context.model.baseTables(entityName)
    val select = query.read.selectStatement(baseTable, Map((schema.ENTITY_ID_COLUMN_NAME, id)))
    cassandra.queryAsync(context.session, select.build, context.executor).map(cassandra.rowToMap, context.executor).toList(context.executor)
  }

  def entityIdToLastValidState(context: Context, before: UUID, entityName: String, id: UUID): Future[Option[Map[String,Object]]] = {
    val states = entityIdToObjectStates(context.queryContext, entityName, id)
    states.flatMap(states => {
      val beforeStates = states.reverse.dropWhile(_(schema.TRANSACTION_ID_COLUMN_NAME).asInstanceOf[UUID].compareTo(before) < 0)
      def isHeadValid(states: List[Map[String,Object]]): Future[Option[Map[String,Object]]] = {
        states.headOption.map(head => {
          val isValid = context.getState(head(schema.TRANSACTION_ID_COLUMN_NAME)).map(_ == TransactionState.SUCCESS)(context.executor)
          isValid.flatMap(isValid => {
            if(isValid) {
              if(head.get(schema.TRANSACTION_DELETED_COLUMN_NAME).map(_.asInstanceOf[java.lang.Boolean]).getOrElse(java.lang.Boolean.FALSE)) {
                Future.successful(Some(head))
              } else {
                Future.successful(None)
              }
            } else {
              isHeadValid(states.tail)
            }
          })(context.executor)
        }).getOrElse(Future.successful(None))
      }
      isHeadValid(beforeStates)
    })(context.executor)
  }
}
