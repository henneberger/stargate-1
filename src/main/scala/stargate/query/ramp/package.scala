package stargate.query

import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import stargate.model.queries._
import stargate.model.{OutputModel, ScalarCondition}
import stargate.query.ramp.read.MaybeReadRows
import stargate.query.ramp.write.WriteOp
import stargate.schema.GroupedConditions
import stargate.util.AsyncList
import stargate.{keywords, query, schema, util}

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
  type MutationResult = Future[Option[(List[Map[String,Object]], List[ramp.write.WriteOp])]]


  def addResponseMetadata(result: MutationResult, metadata: Map[String,Object], executor: ExecutionContext): MutationResult = {
    result.map(_.map(entities_statements => {
      (entities_statements._1.map(entity => entity ++ metadata), entities_statements._2)
    }))(executor)
  }
  def addResponseMetadata(result: MutationResult, key: String, value: String, executor: ExecutionContext): MutationResult = {
    addResponseMetadata(result, Map((key, value)), executor)
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
    rootIds.map(rootIds => util.sequence(rootIds).map(idLists => idLists.map(_.toSet).reduce(_.intersect(_)).toList))(context.executor)
  }



  def resolveRelations(context: Context, transactionId: UUID, entityName: String, relationPath: List[String], ids: ramp.read.MaybeRead[UUID]): ramp.read.MaybeRead[UUID] = {
    if(relationPath.isEmpty) {
      ids
    } else {
      val relationName = relationPath.head
      val next = stargate.util.flattenFOFO(ids.map(_.map(ids => ramp.read.resolveRelationIds(context, transactionId, entityName, ids, relationName)))(context.executor), context.executor)
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



  def relationLink(model: OutputModel, transactionId: UUID, entityName: String, parentId: UUID, relationName: String, payload: List[Map[String,Object]]): List[WriteOp] = {
    payload.flatMap(entity => ramp.write.createBidirectionalRelation(model, transactionId, entityName, relationName, parentId, UUID.fromString(entity(schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[String])))
  }
  def relationUnlink(model: OutputModel, transactionId: UUID, entityName: String, parentId: UUID, relationName: String, payload: List[Map[String,Object]]): List[WriteOp] = {
    def ids(entity: Map[String,Object]): Map[String,Object] = Map((schema.RELATION_FROM_COLUMN_NAME, parentId), (schema.RELATION_TO_COLUMN_NAME, entity(schema.ENTITY_ID_COLUMN_NAME)), (schema.TRANSACTION_ID_COLUMN_NAME, entity(schema.TRANSACTION_ID_COLUMN_NAME)))
    payload.flatMap(entity => ramp.write.deleteBidirectionalRelation(model, transactionId, entityName, relationName, ids(entity)))
  }
  // payload is a list of entities wrapped in link or unlink.  perform whichever link operation is specified between parent ids and child ids
  def relationChange(model: OutputModel, transactionId: UUID, entityName: String, parentId: UUID, relationName: String, payload: List[Map[String,Object]]): List[WriteOp] = {
    val byOperation = payload.groupBy(_(keywords.response.RELATION))
    val linked = byOperation.get(keywords.response.RELATION_LINK).map(relationLink(model, transactionId, entityName, parentId, relationName, _)).getOrElse(List.empty)
    val unlinked = byOperation.get(keywords.response.RELATION_UNLINK).map(relationUnlink(model, transactionId, entityName, parentId, relationName, _)).getOrElse(List.empty)
    linked ++ unlinked
  }

  // perform nested mutation, then take result (child entities wrapped in either link/unlink/replace) and update relations to parent ids
  def mutateAndLinkRelations(context: Context, transactionId: UUID, entityName: String, entityId: UUID, payloadMap: Map[String,RelationMutation]): MutationResult = {
    implicit val executor: ExecutionContext = context.executor
    val entity = context.model.input.entities(entityName)
    val relationMutationResults = payloadMap.toList.map(name_mutation => {
      val (relationName, childMutation) = name_mutation
      val mutationResult = relationMutation(context, transactionId, entityName, entityId, relationName, entity.relations(relationName).targetEntityName, childMutation)
      mutationResult.map(_.map((relationName, _)))
    })
    val sequencedMutationResults = util.sequence(relationMutationResults, executor).map(util.sequence)(executor)
    val relationLinkResults = sequencedMutationResults.map(_.map(relations => {
      val entity: Map[String,Object] = query.write.entityIdPayload(entityId) ++ relations.map(kv => (kv._1, kv._2._1)).toMap
      val changes = relations.map(name_result => {
        relationChange(context.model, transactionId, entityName, entityId, name_result._1, name_result._2._1)
      })
      (List(entity), relations.flatMap(_._2._2) ++ changes.flatten)
    }))
    relationLinkResults
  }

  def createOne(context: Context, transactionId: UUID, entityName: String, payload: CreateOneMutation): MutationResult = {
    val (uuid, creates) = ramp.write.createEntity(context.model.entityTables(entityName), payload.fields.updated(schema.TRANSACTION_ID_COLUMN_NAME, transactionId))
    val linkWrapped = payload.relations.map((rm: (String,Mutation)) => (rm._1, LinkMutation(rm._2)))
    val linkResults = mutateAndLinkRelations(context, transactionId, entityName, uuid, linkWrapped)
    val createResult = linkResults.map(_.map(linkResult => (linkResult._1, creates ++ linkResult._2)))(context.executor)
    addResponseMetadata(createResult, keywords.response.ACTION, keywords.response.ACTION_CREATE, context.executor)
  }

  def create(context: Context, transactionId: UUID, entityName: String, payload: CreateMutation): MutationResult = {
    val creates = payload.creates.map(createOne(context, transactionId, entityName, _))
    util.sequence(creates, context.executor).map(lists => util.sequence(lists).map(data_ops => (data_ops.flatMap(_._1), data_ops.flatMap(_._2))))(context.executor)
  }

  def matchMutation(context: Context, transactionId: UUID, entityName: String, payload: MatchMutation): MutationResult = {
    val ids = matchEntities(context, transactionId, entityName, payload.`match`)
    ids.map(_.map(ids => (ids.map(id => query.write.entityIdPayload(id).updated(keywords.response.ACTION, keywords.response.ACTION_UPDATE)), List.empty)))(context.executor)
  }

  def update(context: Context, transactionId: UUID, entityName: String, ids: ramp.read.MaybeRead[UUID], payload: UpdateMutation): MutationResult = {
    val executor = context.executor
    val updateAll = ids.map(_.map(_.map( id => {
      val state = ramp.read.entityIdToLastValidState(context, transactionId, entityName, id)
      val updateOne = state.map(_.map(_.map(currentEntity => {
        val updates = ramp.write.updateEntity(context.model.entityTables(entityName), currentEntity, payload.fields.updated(schema.TRANSACTION_ID_COLUMN_NAME, transactionId))
        val linkResults = mutateAndLinkRelations(context, transactionId, entityName, id, payload.relations)
        linkResults.map(_.map(linkResult => (linkResult._1, updates ++ linkResult._2)))(executor)
      })))(executor)
      util.flattenFOLFO(updateOne, executor)
    })))(executor)
    val result = util.flattenFOLFO(updateAll, executor)
    result.map(_.map(lists => {
      val flat = lists.flatten
      (flat.flatMap(_._1), flat.flatMap(_._2))
    }))(executor)
  }
  def update(context: Context, transactionId: UUID, entityName: String, payload: UpdateMutation): MutationResult = {
    val ids = matchEntities(context, transactionId, entityName, payload.`match`)
    val result = update(context, transactionId, entityName, ids, payload)
    addResponseMetadata(result, keywords.response.ACTION, keywords.response.ACTION_UPDATE, context.executor)
  }


  def mutation(context: Context, transactionId: UUID, entityName: String, payload: Mutation): MutationResult = {
    payload match {
      case createReq: CreateMutation => create(context, transactionId, entityName, createReq)
      case `match`: MatchMutation => matchMutation(context, transactionId, entityName, `match`)
      case updateReq: UpdateMutation => update(context, transactionId, entityName, updateReq)
    }
  }

  def unlinkObject(relationRow: Map[String,Object]): Map[String,Object] = {
    Map[String,Object]((schema.ENTITY_ID_COLUMN_NAME, relationRow(schema.RELATION_TO_COLUMN_NAME)),
      (schema.TRANSACTION_ID_COLUMN_NAME, relationRow(schema.TRANSACTION_ID_COLUMN_NAME)),
      (keywords.response.ACTION, keywords.response.ACTION_UPDATE),
      (keywords.response.RELATION, keywords.response.RELATION_UNLINK))
  }
  def linkMutation(context: Context, transactionId: UUID, entityName: String, payload: Mutation): MutationResult = {
    val result = mutation(context, transactionId, entityName, payload)
    addResponseMetadata(result, keywords.response.RELATION, keywords.response.RELATION_LINK, context.executor)
  }
  def unlinkMutation(context: Context, transactionId: UUID, parentEntityName: String, parentId: UUID, parentRelation: String, entityName: String, `match`: GroupedConditions[Object]): MutationResult = {
    val matchIds = matchEntities(context, transactionId, entityName, `match`)
    val relations = ramp.read.resolveRelation(context, transactionId, parentEntityName, List(parentId), parentRelation)
    val result = relations.map(_.map(relations => {
      matchIds.map(_.map(matchIds => {
        val matchIdSet = matchIds.toSet
        val matchingRelations = relations.filter(r => matchIdSet(r(schema.RELATION_TO_COLUMN_NAME).asInstanceOf[UUID]))
        (matchingRelations.map(unlinkObject), List.empty)
      }))(context.executor)
    }))(context.executor)
    util.flattenFOFO(result, context.executor)
  }
  def replaceMutation(context: Context, transactionId: UUID, parentEntityName: String, parentId: UUID, parentRelation: String, entityName: String, payload: Mutation): MutationResult = {
    val linkMutationResult = mutation(context, transactionId, entityName, payload)
    val result = linkMutationResult.map(_.map(linked_statements => {
      val (mutationObjects, mutationStatements) = linked_statements
      val linkIds = mutationObjects.map(_(stargate.schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]).toSet
      val relations = ramp.read.resolveRelation(context, transactionId, parentEntityName, List(parentId), parentRelation)
      val unlinkRelations = relations.map(_.map(_.filter(r => linkIds(r(schema.RELATION_TO_COLUMN_NAME).asInstanceOf[UUID]))))(context.executor)
      unlinkRelations.map(_.map(unlinkRelations => {
        val linkObjects = mutationObjects.map(_.updated(keywords.response.RELATION, keywords.response.RELATION_LINK))
        val unlinkObjects = unlinkRelations.map(unlinkObject)
        (linkObjects ++ unlinkObjects, mutationStatements)
      }))(context.executor)
    }))(context.executor)
    util.flattenFOFO(result, context.executor)
  }
  def relationMutation(context: Context, transactionId: UUID, parentEntityName: String, parentId: UUID, parentRelation: String, entityName: String, payload: RelationMutation): MutationResult = {
    payload match {
      case link: LinkMutation => linkMutation(context, transactionId, entityName, link.mutation)
      case unlink: UnlinkMutation => unlinkMutation(context, transactionId, parentEntityName, parentId, parentRelation, entityName, unlink.`match`)
      case replace: ReplaceMutation => replaceMutation(context, transactionId, parentEntityName, parentId, parentRelation, entityName, replace.mutation)
    }
  }

}
