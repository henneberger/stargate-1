package stargate.query

import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import stargate.model.queries.{GetQuery, GetSelection}
import stargate.model.{OutputModel, ScalarCondition}
import stargate.query.pagination.TruncateResult
import stargate.{query, schema}
import stargate.query.ramp.read
import stargate.schema.GroupedConditions
import stargate.util.AsyncList
import stargate.util

import scala.concurrent.{ExecutionContext, Future}

package object ramp {

  object TransactionState extends Enumeration {
    class Value(val name: String, val asInt: Int) extends super.Val(name)
    val IN_PROGRESS = new Value("IN_PROGRESS", 1)
    val SUCCESS = new Value("SUCCESS", 0)
    val FAILED = new Value("FAILED", 2)

    val ints: Map[Int, Value] = this.values.iterator.map(state => {
      val stateValue = state.asInstanceOf[Value]
      (stateValue.asInt, stateValue)
    }).toMap
    def toInt(state: Value): Int = state.asInt
    def fromInt(int: Int): Value = ints(int)
  }

  type GetTransactionState = UUID => Future[TransactionState.Value]
  type SetTransactionState = (UUID, TransactionState.Value) => Future[Unit]

  case class Context(model: OutputModel, getState: GetTransactionState, setState: SetTransactionState, session: CqlSession,executor: ExecutionContext) {
    val queryContext = query.Context(model, session, executor)
  }

  def matchEntities(context: Context, transactionId: UUID, entityName: String, conditions: List[ScalarCondition[Object]]): ramp.read.MaybeReadRows = {
    val executor = context.executor
    val potentialIds = query.matchEntities(context.queryContext, entityName, conditions).dedupe(executor)
    val potentialEntities = potentialIds.map(id => ramp.read.entityIdToLastValidState(context, transactionId, entityName, id), executor)
    ramp.read.flatten(potentialEntities, executor).map(_.map(_.filter(query.read.checkConditions(_, conditions))))(executor)
  }
  def matchEntities(context: Context, transactionId: UUID, entityName: String, conditions: GroupedConditions[Object]): ramp.read.MaybeRead[UUID] = {
    val groupedEntities = conditions.toList.map(path_conds => {
      val (path, conditions) = path_conds
      val targetEntityName = schema.traverseEntityPath(context.model.input.entities, entityName, path)
      (path, matchEntities(context, transactionId, targetEntityName, conditions).map(_.map(_.map(_(schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])))(context.executor))
    }).toMap
    val rootIds = util.sequence(groupedEntities.toList.map(path_ids => resolveReverseRelations(context, transactionId, entityName, path_ids._1, path_ids._2)), context.executor)
    rootIds.map(rootIds => {
      if(rootIds.exists(_.isEmpty)) {
        None
      } else {
        Some(rootIds.map(_.get).map(_.toSet).reduce(_.intersect(_)).toList)
      }
    })(context.executor)
  }



  def resolveRelations(context: Context, transactionId: UUID, entityName: String, relationPath: List[String], ids: ramp.read.MaybeRead[UUID]): ramp.read.MaybeRead[UUID] = {
    if(relationPath.isEmpty) {
      ids
    } else {
      val relationName = relationPath.head
      val next = stargate.util.flattenFOFO(ids.map(_.map(ids => ramp.read.resolveRelation(context, transactionId, entityName, ids, relationName)))(context.executor), context.executor)
      resolveRelations(context, transactionId, context.model.input.entities(entityName).relations(relationName).targetEntityName, relationPath.tail, next)
    }
  }
  def resolveRelations(context: Context, transactionId: UUID, entityName: String, relationPath: List[String], ids: List[UUID]): ramp.read.MaybeRead[UUID] = {
    resolveRelations(context, transactionId, entityName, relationPath, Future.successful(Some(ids)))
  }
  def resolveReverseRelations(context: Context, transactionId: UUID, rootEntityName: String, relationPath: List[String], relatedIds: ramp.read.MaybeRead[UUID]): ramp.read.MaybeRead[UUID] = {
    if(relationPath.isEmpty) {
      relatedIds
    } else {
      val relations = schema.traverseRelationPath(context.model.input.entities, rootEntityName, relationPath).reverse
      val newRootEntityName = relations.head.targetEntityName
      val inversePath = relations.map(_.inverseName)
      resolveRelations(context, transactionId, newRootEntityName, inversePath, relatedIds)
    }
  }
  def resolveReverseRelations(context: Context, transactionId: UUID, entityName: String, relationPath: List[String], ids: List[UUID]): ramp.read.MaybeRead[UUID] = {
    resolveReverseRelations(context, transactionId, entityName, relationPath, Future.successful(Some(ids)))
  }


  def getEntitiesAndRelated(context: Context, transactionId: UUID, entityName: String, ids: ramp.read.MaybeRead[UUID], payload: GetSelection): ramp.read.MaybeReadRows = {
    val executor = context.executor
    val relations = context.model.input.entities(entityName).relations
    val results = ids.map(_.map(_.map(id => {
      val futureMaybeEntity = ramp.read.entityIdToLastValidState(context, transactionId, entityName, id)
      val entityAndRelations = futureMaybeEntity.map(_.map(_.map(entity => {
        val related = payload.relations.toList.map(name_selection => {
          val (relationName, nestedSelection) = name_selection
          val childIds = resolveRelations(context, transactionId, entityName, List(relationName), List(id))
          val recurse = getEntitiesAndRelated(context, transactionId, relations(relationName).targetEntityName, childIds, nestedSelection)
          recurse.map(_.map(result => (relationName, result)))(executor)
        })
        val sequencedRelated = util.sequence(related, executor).map(relations => util.sequence(relations))(executor)
        sequencedRelated.map(_.map(relation_children => entity ++ relation_children))(executor)
      })))(executor)
      util.flattenFOLFO(entityAndRelations, executor)
    })))(executor)
    util.flattenFOLFOL(results, executor)
  }
  def get(context: Context, transactionId: UUID, entityName: String, payload: GetQuery): ramp.read.MaybeReadRows = {
    val ids = matchEntities(context, transactionId, entityName, payload.`match`)
    getEntitiesAndRelated(context, transactionId, entityName, ids, payload.selection)
  }



}
