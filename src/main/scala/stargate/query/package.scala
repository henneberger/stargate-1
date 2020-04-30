package stargate

import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatementBuilder, BatchType, SimpleStatement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.datastax.oss.driver.internal.core.util.Strings
import com.typesafe.scalalogging.Logger
import stargate.model._
import stargate.model.queries._
import stargate.schema.GroupedConditions
import stargate.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}

package object query {

  val logger = Logger("queries")

  type MutationResult = Future[(List[Map[String,Object]], List[SimpleStatement])]
  type RelationMutationResult = Future[(Map[String,List[Map[String,Object]]], List[SimpleStatement])]


  // for a root-entity and relation path, apply selection conditions to get list of ids of the target entity type
  def matchEntities(model: OutputModel, rootEntityName: String, relationPath: List[String], conditions: List[ScalarCondition[Term]], session: CqlSession, executor: ExecutionContext): AsyncList[UUID] = {
    // TODO: when condition is just List((entityId, =, _)) or List((entityId, IN, _)), then return the ids in condition immediately without querying
    val entityName = schema.traverseEntityPath(model.input, rootEntityName, relationPath)
    val entityTables = model.entityTables(entityName)
    val tableScores = schema.tableScores(conditions.map(_.named), entityTables)
    val bestScore = tableScores.keys.min
    // TODO: cache mapping of conditions to best table in OutputModel
    val bestTable = tableScores(bestScore).head
    var selection = read.selectStatement(bestTable.name, conditions)
    if(!bestScore.perfect) {
      logger.warn(s"failed to find index for entity '${entityName}' with conditions ${conditions}, best match was: ${bestTable}")
      selection = selection.allowFiltering
    }
    cassandra.queryAsync(session, selection.build, executor).map(_.getUuid(Strings.doubleQuote(schema.ENTITY_ID_COLUMN_NAME)), executor)
  }

  // starting with an entity type and a set of ids for that type, walk the relation-path to find related entity ids of the final type in the path
  def resolveRelations(model: OutputModel, entityName: String, relationPath: List[String], ids: AsyncList[UUID], session: CqlSession, executor: ExecutionContext): AsyncList[UUID] = {
    if(relationPath.isEmpty) {
      ids
    } else {
      val targetEntityName = model.input.entities(entityName).relations(relationPath.head).targetEntityName
      val relationTable = model.relationTables((entityName, relationPath.head))
      // TODO: possibly use some batching instead of passing one id at a time?
      val nextIds = ids.flatMap(id => read.relatedSelect(relationTable.name, List(id), session, executor), executor)
      resolveRelations(model, targetEntityName, relationPath.tail, nextIds, session, executor)
    }
  }
  // given a list of related ids and relation path, walk the relation-path in reverse to find related entity ids of the root entity type
  def resolveReverseRelations(model: OutputModel, rootEntityName: String, relationPath: List[String], relatedIds: AsyncList[UUID], session: CqlSession, executor: ExecutionContext): AsyncList[UUID] = {
    if(relationPath.isEmpty) {
      relatedIds
    } else {
      val relations = schema.traverseRelationPath(model.input, rootEntityName, relationPath).reverse
      val newRootEntityName = relations.head.targetEntityName
      val inversePath = relations.map(_.inverseName)
      resolveRelations(model, newRootEntityName, inversePath, relatedIds, session, executor)
    }
  }

  // for a root-entity and relation path, apply selection conditions to get related entity ids, then walk relation tables in reverse to get ids of the root entity type
  def matchEntities(model: OutputModel, entityName: String, conditions: GroupedConditions[Object], session: CqlSession, executor: ExecutionContext): AsyncList[UUID] = {
    val groupedEntities = conditions.toList.map(path_conds => {
      val (path, conditions) = path_conds
      val termConditions = conditions.map(cond => cond.replaceArgument[Term](QueryBuilder.literal(cond.argument)))
      (path, matchEntities(model, entityName, path, termConditions, session, executor))
    }).toMap
    val rootIds = groupedEntities.toList.map(path_ids => resolveReverseRelations(model, entityName, path_ids._1, path_ids._2, session, executor))
    // TODO: streaming intersection
    val sets = rootIds.map(_.toList(executor).map(_.toSet)(executor))
    implicit val ec: ExecutionContext = executor
    // technically no point in returning a lazy AsyncList since intersection is being done eagerly - just future proofing for when this is made streaming later
    AsyncList.unfuture(Future.sequence(sets).map(_.reduce(_.intersect(_))).map(set => AsyncList.fromList(set.toList)), executor)
  }


  def getEntitiesAndRelated(model: OutputModel, entityName: String, ids: AsyncList[UUID], payload: GetSelection, session: CqlSession, executor: ExecutionContext): AsyncList[Map[String,Object]] = {
    val relations = model.input.entities(entityName).relations
    val results = ids.map(id => {
      val futureMaybeEntity = read.entityIdToObject(model, entityName, payload.include, id, session, executor)
      futureMaybeEntity.map(_.map(entity => {
        val related = payload.relations.map((name_selection: (String, GetSelection)) => {
          val (relationName, nestedSelection) = name_selection
          val childIds = resolveRelations(model, entityName, List(relationName), AsyncList.singleton(id), session, executor)
          val recurse = getEntitiesAndRelated(model, relations(relationName).targetEntityName, childIds, nestedSelection, session, executor)
          (relationName, recurse)
        })
        entity ++ related
      }))(executor)
    }, executor)
    AsyncList.filterSome(AsyncList.unfuture(results, executor), executor)
  }

  // returns entities matching conditions in payload, with all lists being lazy streams (async list)
  def get(model: OutputModel, entityName: String, payload: GetQuery, session: CqlSession, executor: ExecutionContext): AsyncList[Map[String,Object]] = {
    val ids = matchEntities(model, entityName, payload.`match`, session, executor)
    getEntitiesAndRelated(model, entityName, ids, payload.selection, session, executor)
  }
  // gets entities matching condition, then truncates all entities lists by their "-limit" parameters in the request, and returns the remaining streams in map
  def getAndTruncate(model: OutputModel, entityName: String, payload: GetQuery, defaultLimit: Int, defaultTTL: Int, session: CqlSession, executor: ExecutionContext): Future[(List[Map[String,Object]], pagination.Streams)] = {
    val result = get(model, entityName, payload, session, executor)
    pagination.truncate(model.input, entityName, payload.selection, result, defaultLimit, defaultTTL, executor)
  }
  // same as above, but drops the remaining streams for cases where you dont care
  def getAndTruncate(model: OutputModel, entityName: String, payload: GetQuery, defaultLimit: Int, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    getAndTruncate(model, entityName, payload, defaultLimit, 0, session, executor).map(_._1)(executor)
  }

  def relationLink(model: OutputModel, entityName: String, parentId: UUID, relationName: String, payload: List[Map[String,Object]]): List[SimpleStatement] = {
    payload.map(_(schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]).flatMap(id => write.createBidirectionalRelation(model, entityName, relationName)(parentId, id))
  }
  def relationUnlink(model: OutputModel, entityName: String, parentId: UUID, relationName: String, payload: List[Map[String,Object]]): List[SimpleStatement] = {
    payload.map(_(schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]).flatMap(id => write.deleteBidirectionalRelation(model, entityName, relationName)(parentId, id))
  }
  // payload is a list of entities wrapped in link or unlink.  perform whichever link operation is specified between parent ids and child ids
  def relationChange(model: OutputModel, entityName: String, parentId: UUID, relationName: String, payload: Map[String,List[Map[String,Object]]]): List[SimpleStatement] = {
    val linked = payload.get(keywords.relation.LINK).map(relationLink(model, entityName, parentId, relationName, _)).getOrElse(List.empty)
    val unlinked = payload.get(keywords.relation.UNLINK).map(relationUnlink(model, entityName, parentId, relationName, _)).getOrElse(List.empty)
    linked ++ unlinked
  }

  // perform nested mutation, then take result (child entities wrapped in either link/unlink/replace) and update relations to parent ids
  def mutateAndLinkRelations(model: OutputModel, entityName: String, entityId: UUID, payloadMap: Map[String,RelationMutation], session: CqlSession, executor: ExecutionContext): MutationResult = {
    implicit val ec: ExecutionContext = executor
    val entity = model.input.entities(entityName)
    val relationMutationResults = payloadMap.map((name_mutation: (String, RelationMutation)) => {
      val (relationName, childMutation) = name_mutation
      (relationName, relationMutation(model, entityName, entityId, relationName, entity.relations(relationName).targetEntityName, childMutation, session, executor))
    })
    val relationLinkResults = relationMutationResults.map((name_result: (String,RelationMutationResult)) => {
      val (relationName, result) = name_result
      result.map(relations_statements => {
        val (relationChanges, mutationStatements) = relations_statements
        val relationStatements = relationChange(model, entityName, entityId, relationName, relationChanges)
        ((relationName, relationChanges), relationStatements ++ mutationStatements)
      })(executor)
    })
    Future.sequence(relationLinkResults).map(relations_statements => {
      val (relations, statements) = relations_statements.unzip
      val entity: List[Map[String,Object]] = List(write.entityIdPayload(entityId) ++ relations.toMap)
      (entity, statements.toList.flatten)
    })
  }

  def createOne(model: OutputModel, entityName: String, payload: CreateOneMutation, session: CqlSession, executor: ExecutionContext): MutationResult = {
    val (uuid, creates) = write.createEntity(model.entityTables(entityName), payload)
    val linkWrapped = payload.relations.map((rm: (String,Mutation)) => (rm._1, LinkMutation(rm._2)))
    val linkResults = mutateAndLinkRelations(model, entityName, uuid, linkWrapped, session, executor)
    linkResults.map(linkResult => (linkResult._1, creates ++ linkResult._2))(executor)
  }

  def create(model: OutputModel, entityName: String, payload: CreateMutation, session: CqlSession, executor: ExecutionContext): MutationResult = {
    val creates = payload.creates.map(createOne(model, entityName, _, session, executor))
    implicit val ec: ExecutionContext = executor
    Future.sequence(creates).map(lists => (lists.flatMap(_._1), lists.flatMap(_._2)))
  }

  def matchMutation(model: OutputModel, entityName: String, payload: MatchMutation, session: CqlSession, executor: ExecutionContext): MutationResult = {
    matchEntities(model, entityName, payload.`match`, session, executor).toList(executor).map(ids => (ids.map(write.entityIdPayload), List.empty[SimpleStatement]))(executor)
  }

  def update(model: OutputModel, entityName: String, ids: AsyncList[UUID], payload: UpdateMutation, session: CqlSession, executor: ExecutionContext): MutationResult = {
    val results = ids.map(id => {
      val futureMaybeEntity = read.entityIdToObject(model, entityName, id, session, executor)
      futureMaybeEntity.map(_.map(currentEntity => {
        val updates = write.updateEntity(model.entityTables(entityName), currentEntity, payload.fields)
        val linkResults = mutateAndLinkRelations(model, entityName, id, payload.relations, session, executor)
        linkResults.map(linkResult => (linkResult._1, updates ++ linkResult._2))(executor)
      }))(executor)
    }, executor)
    val filtered = AsyncList.unfuture(AsyncList.filterSome(AsyncList.unfuture(results, executor), executor), executor).toList(executor)
    filtered.map(lists => (lists.flatMap(_._1), lists.flatMap(_._2)))(executor)
  }

  def update(model: OutputModel, entityName: String, payload: UpdateMutation, session: CqlSession, executor: ExecutionContext): MutationResult = {
    val ids = matchEntities(model, entityName, payload.`match`, session, executor)
    update(model, entityName, ids, payload, session, executor)
  }

  def delete(model: OutputModel, entityName: String, ids: AsyncList[UUID], payload: DeleteSelection, session: CqlSession, executor: ExecutionContext): MutationResult = {
    implicit val ec: ExecutionContext = executor
    val relations = model.input.entities(entityName).relations
    val results = ids.map(id => {
      val futureMaybeEntity = read.entityIdToObject(model, entityName, id, session, executor)
      futureMaybeEntity.map(_.map(entity => {
        val deleteCurrent = write.deleteEntity(model.entityTables(entityName),entity)
        val childResults = relations.map((name_relation:(String, RelationField)) => {
          val (relationName, relation) = name_relation
          val childIds = resolveRelations(model, entityName, List(relationName), AsyncList.singleton(id), session, executor)
          // TODO: dont double delete inverse relations
          val unlinks = childIds.map(childId => write.deleteBidirectionalRelation(model, entityName, relationName)(id, childId), executor).toList(executor)
          val recurse = if(payload.relations.contains(relationName)) {
            delete(model, relation.targetEntityName, childIds, payload.relations(relationName), session, executor).map(x => (List((relationName, x._1)), x._2))
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

  def delete(model: OutputModel, entityName: String, payload: DeleteQuery, session: CqlSession, executor: ExecutionContext): MutationResult = {
    val ids = matchEntities(model, entityName, payload.`match`, session, executor)
    delete(model, entityName, ids, payload.selection, session, executor)
  }


  def mutation(model: OutputModel, entityName: String, payload: Mutation, session: CqlSession, executor: ExecutionContext): MutationResult = {
    payload match {
      case createReq: CreateMutation => create(model, entityName, createReq, session, executor)
      case `match`: MatchMutation => matchMutation(model, entityName, `match`, session, executor)
      case updateReq: UpdateMutation => update(model, entityName, updateReq, session, executor)
    }
  }

  def linkMutation(model: OutputModel, entityName: String, payload: Mutation, session: CqlSession, executor: ExecutionContext): RelationMutationResult = {
    mutation(model, entityName, payload, session, executor).map(x => (Map((keywords.relation.LINK, x._1)), x._2))(executor)
  }
  def unlinkMutation(model: OutputModel, entityName: String, `match`: MatchMutation, session: CqlSession, executor: ExecutionContext): RelationMutationResult = {
    matchMutation(model, entityName, `match`, session, executor).map(x => (Map((keywords.relation.UNLINK, x._1)), x._2))(executor)
  }
  def replaceMutation(model: OutputModel, parentEntityName: String, parentId: UUID, parentRelation: String, entityName: String, payload: Mutation, session: CqlSession, executor: ExecutionContext): RelationMutationResult = {
    val linkMutationResult = mutation(model, entityName, payload, session, executor)
    linkMutationResult.flatMap(linked_statements => {
      val (linkObjects, mutationStatements) = linked_statements
      val linkIds = linkObjects.map(_(stargate.schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]).toSet
      val relatedIds = resolveRelations(model, parentEntityName, List(parentRelation), AsyncList.singleton(parentId), session, executor)
      val unlinkIds = relatedIds.toList(executor).map(_.toSet.diff(linkIds).toList)(executor)
      unlinkIds.map(unlinkIds => {
        val unlinkObjects = unlinkIds.map(write.entityIdPayload)
        (Map((keywords.relation.LINK, linkObjects),(keywords.relation.UNLINK, unlinkObjects)), mutationStatements)
      })(executor)
    })(executor)

  }
  // perform nested mutation (create/update/match), then wrap resulting entity ids with link/unlink/replace to be handled by parent entity
  def relationMutation(model: OutputModel, parentEntityName: String, parentId: UUID, parentRelation: String, entityName: String, payload: RelationMutation, session: CqlSession, executor: ExecutionContext): RelationMutationResult = {
    payload match {
      case link: LinkMutation => linkMutation(model, entityName, link.mutation, session, executor)
      case unlink: UnlinkMutation => unlinkMutation(model, entityName, MatchMutation(unlink.`match`), session, executor)
      case replace: ReplaceMutation => replaceMutation(model, parentEntityName, parentId, parentRelation, entityName, replace.mutation, session, executor)
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
  def createUnbatched(model: OutputModel, entityName: String, payload: CreateMutation, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    writeUnbatched(create(model, entityName, payload, session, executor), session, executor)
  }
  def updateUnbatched(model: OutputModel, entityName: String, payload: UpdateMutation, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    writeUnbatched(update(model, entityName, payload, session, executor), session, executor)
  }
  def deleteUnbatched(model: OutputModel, entityName: String, payload: DeleteQuery, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    writeUnbatched(delete(model, entityName, payload, session, executor), session, executor)
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
  def createBatched(model: OutputModel, entityName: String, payload: CreateMutation, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    writeBatched(create(model, entityName, payload, session, executor), session, executor)
  }
  def updateBatched(model: OutputModel, entityName: String, payload: UpdateMutation, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    writeBatched(update(model, entityName, payload, session, executor), session, executor)
  }
  def deleteBatched(model: OutputModel, entityName: String, payload: DeleteQuery, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    writeBatched(delete(model, entityName, payload, session, executor), session, executor)
  }


}
