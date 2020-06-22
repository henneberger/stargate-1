/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stargate

import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatementBuilder, BatchType, Row, SimpleStatement}
import com.datastax.oss.driver.internal.core.util.Strings
import com.typesafe.scalalogging.Logger
import stargate.model._
import stargate.model.queries._
import stargate.query.pagination.TruncateResult
import stargate.schema.GroupedConditions
import stargate.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}

package object query {

  val logger = Logger("queries")

  case class Context(model: OutputModel, session: CqlSession, executor: ExecutionContext)
  type MutationResult = Future[(List[Map[String,Object]], List[SimpleStatement])]

  def addResponseMetadata(result: MutationResult, key: String, value: String, executor: ExecutionContext): MutationResult = {
    result.map(entities_statements => {
      (entities_statements._1.map(entity => entity.updated(key, value)), entities_statements._2)
    })(executor)
  }

  // for a single entity, apply selection conditions to get list of ids of the target entity type
  def matchEntities(context: Context, entityName: String, conditions: List[ScalarCondition[Object]]): AsyncList[UUID] = {
    // TODO: when condition is just List((entityId, =, _)) or List((entityId, IN, _)), then return the ids in condition immediately without querying
    val entityTables = context.model.entityTables(entityName)
    val tableScores = schema.tableScores(conditions.map(_.named), entityTables)
    val bestScore = tableScores.keys.min
    // TODO: cache mapping of conditions to best table in OutputModel
    val bestTable = tableScores(bestScore).head
    var selection = read.selectStatement(bestTable, conditions)
    if(!bestScore.perfect) {
      logger.warn(s"failed to find index for entity '${entityName}' with conditions ${conditions}, best match was: ${bestTable}")
      selection = selection.allowFiltering
    }
    cassandra.queryAsync(context.session, selection.build, context.executor).map(_.getUuid(Strings.doubleQuote(schema.ENTITY_ID_COLUMN_NAME)), context.executor)
  }
  // for a root-entity and relation path, apply selection conditions to get related entity ids, then walk relation tables in reverse to get ids of the root entity type
  def matchEntities(context: Context, entityName: String, conditions: GroupedConditions[Object]): AsyncList[UUID] = {
    val groupedEntities = conditions.toList.map(path_conds => {
      val (path, conditions) = path_conds
      val targetEntityName = schema.traverseEntityPath(context.model.input.entities, entityName, path)
      (path, matchEntities(context, targetEntityName, conditions))
    }).toMap
    val rootIds = groupedEntities.toList.map(path_ids => resolveReverseRelations(context, entityName, path_ids._1, path_ids._2))
    // TODO: streaming intersection
    val sets = rootIds.map(_.toList(context.executor).map(_.toSet)(context.executor))
    implicit val ec: ExecutionContext = context.executor
    // technically no point in returning a lazy AsyncList since intersection is being done eagerly - just future proofing for when this is made streaming later
    AsyncList.unfuture(Future.sequence(sets).map(_.reduce(_.intersect(_))).map(set => AsyncList.fromList(set.toList)), context.executor)
  }

  // starting with an entity type and a set of ids for that type, walk the relation-path to find related entity ids of the final type in the path
  def resolveRelations(context: Context, entityName: String, relationPath: List[String], ids: AsyncList[UUID]): AsyncList[UUID] = {
    if(relationPath.isEmpty) {
      ids
    } else {
      val targetEntityName = context.model.input.entities(entityName).relations(relationPath.head).targetEntityName
      val relationTable = context.model.relationTables((entityName, relationPath.head))
      // TODO: possibly use some batching instead of passing one id at a time?
      val nextIds = ids.flatMap(id => read.relatedSelect(relationTable.keyspace, relationTable.name, List(id), context.session, context.executor), context.executor)
      resolveRelations(context, targetEntityName, relationPath.tail, nextIds)
    }
  }
  // given a list of related ids and relation path, walk the relation-path in reverse to find related entity ids of the root entity type
  def resolveReverseRelations(context: Context, rootEntityName: String, relationPath: List[String], relatedIds: AsyncList[UUID]): AsyncList[UUID] = {
    if(relationPath.isEmpty) {
      relatedIds
    } else {
      val relations = schema.traverseRelationPath(context.model.input.entities, rootEntityName, relationPath).reverse
      val newRootEntityName = relations.head.targetEntityName
      val inversePath = relations.map(_.inverseName)
      resolveRelations(context, newRootEntityName, inversePath, relatedIds)
    }
  }

  def getEntitiesAndRelated(context: Context, entityName: String, ids: AsyncList[UUID], payload: GetSelection): AsyncList[Map[String,Object]] = {
    val relations = context.model.input.entities(entityName).relations
    val results = ids.map(id => {
      val futureMaybeEntity = read.entityIdToObject(context, entityName, payload.include, id)
      futureMaybeEntity.map(_.map(entity => {
        val related = payload.relations.map((name_selection: (String, GetSelection)) => {
          val (relationName, nestedSelection) = name_selection
          val childIds = resolveRelations(context, entityName, List(relationName), AsyncList.singleton(id))
          val recurse = getEntitiesAndRelated(context, relations(relationName).targetEntityName, childIds, nestedSelection)
          (relationName, recurse)
        })
        entity ++ related
      }))(context.executor)
    }, context.executor)
    AsyncList.filterSome(AsyncList.unfuture(results, context.executor), context.executor)
  }

  // returns entities matching conditions in payload, with all lists being lazy streams (async list)
  def get(context: Context, entityName: String, payload: GetQuery): AsyncList[Map[String,Object]] = {
    val ids = matchEntities(context, entityName, payload.`match`)
    getEntitiesAndRelated(context, entityName, ids, payload.selection)
  }
  // gets entities matching condition, then truncates all entities lists by their "-limit" parameters in the request, and returns the remaining streams in map
  def getAndTruncate(context: Context, entityName: String, payload: GetQuery, defaultLimit: Int, defaultTTL: Int): TruncateResult = {
    val result = get(context, entityName, payload)
    pagination.truncate(context.model.input, entityName, payload.selection, result, defaultLimit, defaultTTL, context.executor)
  }
  // same as above, but drops the remaining streams for cases where you dont care
  def getAndTruncate(context: Context, entityName: String, payload: GetQuery, defaultLimit: Int): Future[List[Map[String,Object]]] = {
    getAndTruncate(context, entityName, payload, defaultLimit, 0).map(_._1)(context.executor)
  }

  def relationLink(model: OutputModel, entityName: String, parentId: UUID, relationName: String, payload: List[Map[String,Object]]): List[SimpleStatement] = {
    payload.map(_(schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]).flatMap(id => write.createBidirectionalRelation(model, entityName, relationName)(parentId, id))
  }
  def relationUnlink(model: OutputModel, entityName: String, parentId: UUID, relationName: String, payload: List[Map[String,Object]]): List[SimpleStatement] = {
    payload.map(_(schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]).flatMap(id => write.deleteBidirectionalRelation(model, entityName, relationName)(parentId, id))
  }
  // payload is a list of entities wrapped in link or unlink.  perform whichever link operation is specified between parent ids and child ids
  def relationChange(model: OutputModel, entityName: String, parentId: UUID, relationName: String, payload: List[Map[String,Object]]): List[SimpleStatement] = {
    val byOperation = payload.groupBy(_(keywords.response.RELATION))
    val linked = byOperation.get(keywords.response.RELATION_LINK).map(relationLink(model, entityName, parentId, relationName, _)).getOrElse(List.empty)
    val unlinked = byOperation.get(keywords.response.RELATION_UNLINK).map(relationUnlink(model, entityName, parentId, relationName, _)).getOrElse(List.empty)
    linked ++ unlinked
  }

  // perform nested mutation, then take result (child entities wrapped in either link/unlink/replace) and update relations to parent ids
  def mutateAndLinkRelations(context: Context, entityName: String, entityId: UUID, payloadMap: Map[String,RelationMutation]): MutationResult = {
    implicit val ec: ExecutionContext = context.executor
    val entity = context.model.input.entities(entityName)
    val relationMutationResults = payloadMap.map((name_mutation: (String, RelationMutation)) => {
      val (relationName, childMutation) = name_mutation
      (relationName, relationMutation(context, entityName, entityId, relationName, entity.relations(relationName).targetEntityName, childMutation))
    })
    val relationLinkResults = relationMutationResults.map((name_result: (String,MutationResult)) => {
      val (relationName, result) = name_result
      result.map(relations_statements => {
        val (relationChanges, mutationStatements) = relations_statements
        val relationStatements = relationChange(context.model, entityName, entityId, relationName, relationChanges)
        ((relationName, relationChanges), relationStatements ++ mutationStatements)
      })
    })
    Future.sequence(relationLinkResults).map(relations_statements => {
      val (relations, statements) = relations_statements.unzip
      val entity: List[Map[String,Object]] = List(write.entityIdPayload(entityId) ++ relations.toMap)
      (entity, statements.toList.flatten)
    })
  }

  def createOne(context: Context, entityName: String, payload: CreateOneMutation): MutationResult = {
    val (uuid, creates) = write.createEntity(context.model.entityTables(entityName), payload.fields)
    val linkWrapped = payload.relations.map((rm: (String,Mutation)) => (rm._1, LinkMutation(rm._2)))
    val linkResults = mutateAndLinkRelations(context, entityName, uuid, linkWrapped)
    val createResult = linkResults.map(linkResult => (linkResult._1, creates ++ linkResult._2))(context.executor)
    addResponseMetadata(createResult, keywords.response.ACTION, keywords.response.ACTION_CREATE, context.executor)
  }

  def create(context: Context, entityName: String, payload: CreateMutation): MutationResult = {
    val creates = payload.creates.map(createOne(context, entityName, _))
    implicit val ec: ExecutionContext = context.executor
    Future.sequence(creates).map(lists => (lists.flatMap(_._1), lists.flatMap(_._2)))
  }

  def matchMutation(context: Context, entityName: String, payload: MatchMutation): MutationResult = {
    val ids = matchEntities(context, entityName, payload.`match`).toList(context.executor)
    ids.map(ids => (ids.map(id => write.entityIdPayload(id).updated(keywords.response.ACTION, keywords.response.ACTION_UPDATE)), List.empty[SimpleStatement]))(context.executor)
  }

  def update(context: Context, entityName: String, ids: AsyncList[UUID], payload: UpdateMutation): MutationResult = {
    val executor = context.executor
    val results = ids.map(id => {
      val futureMaybeEntity = read.entityIdToObject(context, entityName, id)
      futureMaybeEntity.map(_.map(currentEntity => {
        val updates = write.updateEntity(context.model.entityTables(entityName), currentEntity, payload.fields)
        val linkResults = mutateAndLinkRelations(context, entityName, id, payload.relations)
        linkResults.map(linkResult => (linkResult._1, updates ++ linkResult._2))(executor)
      }))(executor)
    }, executor)
    val filtered = AsyncList.unfuture(AsyncList.filterSome(AsyncList.unfuture(results, executor), executor), executor).toList(executor)
    filtered.map(lists => (lists.flatMap(_._1), lists.flatMap(_._2)))(executor)
  }

  def update(context: Context, entityName: String, payload: UpdateMutation): MutationResult = {
    val ids = matchEntities(context, entityName, payload.`match`)
    val result = update(context, entityName, ids, payload)
    addResponseMetadata(result, keywords.response.ACTION, keywords.response.ACTION_UPDATE, context.executor)
  }

  def delete(context: Context, entityName: String, ids: AsyncList[UUID], payload: DeleteSelection): MutationResult = {
    implicit val executor: ExecutionContext = context.executor
    val relations = context.model.input.entities(entityName).relations
    val results = ids.map(id => {
      val futureMaybeEntity = read.entityIdToObject(context, entityName, id)
      futureMaybeEntity.map(_.map(entity => {
        val deleteCurrent = write.deleteEntity(context.model.entityTables(entityName),entity)
        val childResults = relations.map((name_relation:(String, RelationField)) => {
          val (relationName, relation) = name_relation
          val childIds = resolveRelations(context, entityName, List(relationName), AsyncList.singleton(id))
          // TODO: dont double delete inverse relations
          val unlinks = childIds.map(childId => write.deleteBidirectionalRelation(context.model, entityName, relationName)(id, childId), executor).toList(executor)
          val recurse = if(payload.relations.contains(relationName)) {
            delete(context, relation.targetEntityName, childIds, payload.relations(relationName)).map(x => (List((relationName, x._1)), x._2))
          } else {
            Future.successful((List.empty, List.empty))
          }
          // make recurse-delete future depend on links being deleted
          unlinks.flatMap(unlinks => recurse.map(recurse => (recurse._1, unlinks.flatten ++ recurse._2) ))
        })
        Future.sequence(childResults).map(relations_statements => {
          val (relations, statements) = relations_statements.unzip
          (write.entityIdPayload(id) ++ relations.flatten.toMap, deleteCurrent ++ statements.flatten)
        })
      }))(executor)
    }, executor)
    val filtered = AsyncList.unfuture(AsyncList.filterSome(AsyncList.unfuture(results, executor), executor), executor).toList(executor)
    filtered.map(lists => (lists.map(_._1), lists.flatMap(_._2)))
  }

  def delete(context: Context, entityName: String, payload: DeleteQuery): MutationResult = {
    val ids = matchEntities(context, entityName, payload.`match`)
    val result = delete(context, entityName, ids, payload.selection)
    addResponseMetadata(result, keywords.response.ACTION, keywords.response.ACTION_DELETE, context.executor)
  }


  def mutation(context: Context, entityName: String, payload: Mutation): MutationResult = {
    payload match {
      case createReq: CreateMutation => create(context, entityName, createReq)
      case `match`: MatchMutation => matchMutation(context, entityName, `match`)
      case updateReq: UpdateMutation => update(context, entityName, updateReq)
    }
  }

  def linkMutation(context: Context, entityName: String, payload: Mutation): MutationResult = {
    val result = mutation(context, entityName, payload)
    addResponseMetadata(result, keywords.response.RELATION, keywords.response.RELATION_LINK, context.executor)
  }
  def unlinkMutation(context: Context, entityName: String, `match`: MatchMutation): MutationResult = {
    val result = matchMutation(context, entityName, `match`)
    addResponseMetadata(result, keywords.response.RELATION, keywords.response.RELATION_UNLINK, context.executor)
  }
  def replaceMutation(context: Context, parentEntityName: String, parentId: UUID, parentRelation: String, entityName: String, payload: Mutation): MutationResult = {
    val linkMutationResult = mutation(context, entityName, payload)
    linkMutationResult.flatMap(linked_statements => {
      val (mutationObjects, mutationStatements) = linked_statements
      val linkIds = mutationObjects.map(_(stargate.schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]).toSet
      val relatedIds = resolveRelations(context, parentEntityName, List(parentRelation), AsyncList.singleton(parentId))
      val unlinkIds = relatedIds.toList(context.executor).map(_.toSet.diff(linkIds).toList)(context.executor)
      unlinkIds.map(unlinkIds => {
        val linkObjects = mutationObjects.map(_.updated(keywords.response.RELATION, keywords.response.RELATION_LINK))
        val unlinkObjects = unlinkIds.map(id => write.entityIdPayload(id) ++ Map((keywords.response.ACTION, keywords.response.ACTION_UPDATE), (keywords.response.RELATION, keywords.response.RELATION_UNLINK)))
        (linkObjects ++ unlinkObjects, mutationStatements)
      })(context.executor)
    })(context.executor)
  }
  // TODO: also include link statements in result (instead of appending them separately afterwards)
  // perform nested mutation (create/update/match), then wrap resulting entity ids with link/unlink/replace to be handled by parent entity
  def relationMutation(context: Context, parentEntityName: String, parentId: UUID, parentRelation: String, entityName: String, payload: RelationMutation): MutationResult = {
    payload match {
      case link: LinkMutation => linkMutation(context, entityName, link.mutation)
      case unlink: UnlinkMutation => unlinkMutation(context, entityName, MatchMutation(unlink.`match`))
      case replace: ReplaceMutation => replaceMutation(context, parentEntityName, parentId, parentRelation, entityName, replace.mutation)
    }
  }


  def writeUnbatched(result: MutationResult, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    result.flatMap(entities_statements => {
      val (entities, statements) = entities_statements
      val results = statements.map(cassandra.executeAsync(session, _, executor))
      implicit val ec: ExecutionContext = executor
      Future.sequence(results).map(_ => entities)
    })(executor)
  }
  def createUnbatched(context: Context, entityName: String, payload: CreateMutation): Future[List[Map[String,Object]]] = {
    writeUnbatched(create(context, entityName, payload), context.session, context.executor)
  }
  def updateUnbatched(context: Context, entityName: String, payload: UpdateMutation): Future[List[Map[String,Object]]] = {
    writeUnbatched(update(context, entityName, payload), context.session, context.executor)
  }
  def deleteUnbatched(context: Context, entityName: String, payload: DeleteQuery): Future[List[Map[String,Object]]] = {
    writeUnbatched(delete(context, entityName, payload), context.session, context.executor)
  }
  def writeBatched(result: MutationResult, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    result.flatMap(entities_statements => {
      val (entities, statements) = entities_statements
      val builder = new BatchStatementBuilder(BatchType.LOGGED)
      val batch = statements.foldLeft(builder)((batch, statement) => batch.addStatement(statement)).build
      val results = cassandra.executeAsync(session, batch, executor)
      results.map(_ => entities)(executor)
    })(executor)
  }
  def createBatched(context: Context, entityName: String, payload: CreateMutation): Future[List[Map[String,Object]]] = {
    writeBatched(create(context, entityName, payload), context.session, context.executor)
  }
  def updateBatched(context: Context, entityName: String, payload: UpdateMutation): Future[List[Map[String,Object]]] = {
    writeBatched(update(context, entityName, payload), context.session, context.executor)
  }
  def deleteBatched(context: Context, entityName: String, payload: DeleteQuery): Future[List[Map[String,Object]]] = {
    writeBatched(delete(context, entityName, payload), context.session, context.executor)
  }


}
