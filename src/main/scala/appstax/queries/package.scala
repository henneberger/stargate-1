package appstax

import java.util.UUID

import appstax.cassandra.{CassandraFunction, _}
import appstax.model._
import appstax.util.AsyncList
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.datastax.oss.driver.internal.core.util.Strings
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

package object queries {

  val logger = Logger("queries")

  val MUTATION_OP_CREATE: String = "create"
  val MUTATION_OP_UPDATE: String = "update"
  val MUTATION_OP_MATCH: String = "match"
  val MUTATION_OP_DELETE: String = "delete"

  val RELATION_OP_LINK: String = "link"
  val RELATION_OP_UNLINK: String = "unlink"
  val RELATION_OP_REPLACE: String = "replace"

  val updateRelationFunctions: Map[String, (OutputModel, String, List[UUID], String, Object, CqlSession, ExecutionContext) => Future[Object]] = Map(
    (RELATION_OP_LINK, relationLinkOne),
    (RELATION_OP_UNLINK, relationUnlinkOne)
  )


  type CassandraPagedFunction[I,O] = CassandraFunction[I, PagedResults[O]]
  type CassandraGetFunction[T] = CassandraPagedFunction[Map[String, Object], T]


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
    cassandra.executeAsync(session, selection.build, executor).map(_.getUuid(Strings.doubleQuote(schema.ENTITY_ID_COLUMN_NAME)), executor)
  }

  // starting with an entity type and a set of ids for that type, walk the relation-path to find related entity ids of the final type in the path
  def resolveRelations(model: OutputModel, entityName: String, relationPath: List[String], ids: AsyncList[UUID], session: CqlSession, executor: ExecutionContext): AsyncList[UUID] = {
    if(relationPath.isEmpty) {
      ids
    } else {
      val targetEntityName = model.input.entities(entityName).relations(relationPath.head).targetEntityName
      val relationTable = model.relationTables((entityName, relationPath.head))
      val select = read.relatedSelect(relationTable.name)
      // TODO: possibly use some batching instead of passing one id at a time?
      val nextIds = ids.flatMap(id => select(session, List(id), executor), executor)
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

  // TODO: currently supports only one list of AND'ed triples (column, comparison, value)
  // maybe change this to structured objects, and add support for both AND and OR
  def parseConditions(payload: Object): List[ScalarCondition[Object]] = {
    def parseCondition(col_op_val: List[Object]) = {
      val column :: comparison :: value :: _ = col_op_val
      ScalarCondition(column.asInstanceOf[String], ScalarComparison.fromString(comparison.toString), value)
    }
    val tokens = payload.asInstanceOf[List[Object]]
    tokens.grouped(3).map(parseCondition).toList
  }

  // for a root-entity and relation path, apply selection conditions to get related entity ids, then walk relation tables in reverse to get ids of the root entity type
  def matchEntities(model: OutputModel, entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): AsyncList[UUID] = {
    val conditions = parseConditions(payload)
    val groupedConditions = schema.groupConditionsByPath(conditions, (x:ScalarCondition[Object]) => x.field)
    val nonEmptyGroupedConditions = if(groupedConditions.isEmpty) Map((List(), List())) else groupedConditions
    val trimmedGroupedConditions = nonEmptyGroupedConditions.view.mapValues(_.map(_.trimRelationPath)).toMap
    val groupedEntities = trimmedGroupedConditions.toList.map(path_conds => {
      val (path, conditions) = path_conds
      val targetEntity = model.input.entities(schema.traverseEntityPath(model.input, entityName, path))
      // try to convert passed in comparison arguments to appropriate type for column - e.g. uuids may be passed in as strings from JSON, but then converted to java.util.UUID here
      val termConditions = conditions.map(cond => ScalarCondition[Term](cond.field, cond.comparison, QueryBuilder.literal(targetEntity.fields(cond.field).scalarType.convert(cond.argument))))
      (path, matchEntities(model, entityName, path, termConditions, session, executor))
    }).toMap
    val rootIds = groupedEntities.toList.map(path_ids => resolveReverseRelations(model, entityName, path_ids._1, path_ids._2, session, executor))
    // TODO: streaming intersection
    val sets = rootIds.map(_.toList(executor).map(_.toSet)(executor))
    implicit val ec: ExecutionContext = executor
    // technically no point in returning a lazy AsyncList since intersection is being done eagerly - just future proofing for when this is made streaming later
    AsyncList.unfuture(Future.sequence(sets).map(_.reduce(_.intersect(_))).map(set => AsyncList.fromList(set.toList)), executor)
  }


  def getEntitiesAndRelated(model: OutputModel, entityName: String, ids: AsyncList[UUID], payload: Map[String, Object], session: CqlSession, executor: ExecutionContext): AsyncList[Map[String,Object]] = {
    val relations = model.input.entities(entityName).relations
    val traverseRelations = relations.view.filterKeys(payload.contains).toMap
    val results = ids.map(id => {
      val futureMaybeEntity = read.entityIdToObject(model, entityName, id, session, executor)
      futureMaybeEntity.map(_.map(entity => {
        val related = traverseRelations.map((name_relation: (String, RelationField)) => {
          val (relationName, relation) = name_relation
          val childIds = resolveRelations(model, entityName, List(relationName), AsyncList.singleton(id), session, executor)
          val recurse = getEntitiesAndRelated(model, relation.targetEntityName, childIds, payload(relationName).asInstanceOf[Map[String, Object]], session, executor)
          (relationName, recurse)
        })
        entity ++ related
      }))(executor)
    }, executor)
    AsyncList.filterSome(AsyncList.unfuture(results, executor), executor)
  }

  def get(model: OutputModel, entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): AsyncList[Map[String,Object]] = {
    val payloadMap = payload.asInstanceOf[Map[String,Object]]
    val conditions = payloadMap(MUTATION_OP_MATCH)
    val ids = matchEntities(model, entityName, conditions, session, executor)
    getEntitiesAndRelated(model, entityName, ids, payloadMap, session, executor)
  }



  def relationUpdateOne(update: (OutputModel, String, String) => CassandraFunction[(UUID,UUID), Future[Any]],
                       model: OutputModel, entityName: String, parentIds: List[UUID], relationName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Object] = {
    Try({
      val payloadMap = payload.asInstanceOf[Map[String,Object]]
      val targetId = payloadMap(schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]
      val linkFunc = update(model, entityName, relationName)
      val linkResults = parentIds.map(parentId => linkFunc(session, (parentId, targetId), executor))
      implicit val ec: ExecutionContext = executor
      Future.sequence(linkResults).map(_ => payload)
    }).transform(Success.apply,
      // if run into exception (likely from casting), wrap it with a little more context
      ex => Failure(new RuntimeException(s"failed to update relation '${entityName}.${relationName}', with payload of type '${payload.getClass}', " +
        s"must be Map containing key '${schema.ENTITY_ID_COLUMN_NAME}' with UUID value", ex))).get
  }
  def relationLinkOne(model: OutputModel, entityName: String, parentIds: List[UUID], relationName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Object] = {
    relationUpdateOne(write.createBidirectionalRelation, model, entityName, parentIds, relationName, payload, session, executor)
  }
  def relationUnlinkOne(model: OutputModel, entityName: String, parentIds: List[UUID], relationName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Object] = {
    relationUpdateOne(write.deleteBidirectionalRelation, model, entityName, parentIds, relationName, payload, session, executor)
  }

  def relationUpdate(relationOp: String, model: OutputModel, entityName: String, parentIds: List[UUID], relationName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Map[String, Object]] = {
    val isList = payload.isInstanceOf[List[Object]]
    val payloadList: List[Object] = if(isList) payload.asInstanceOf[List[Object]] else List(payload)
    val updateOne = updateRelationFunctions(relationOp)
    val result = payloadList.map(targetEntity => updateOne(model, entityName, parentIds, relationName, targetEntity, session, executor))
    implicit val ec: ExecutionContext = executor
    Future.sequence(result).map(updated => Map((relationOp, updated)))
  }
  def relationLink(model: OutputModel, entityName: String, parentIds: List[UUID], relationName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Map[String, Object]] = {
    relationUpdate(RELATION_OP_LINK, model, entityName, parentIds, relationName, payload, session, executor)
  }
  def relationUnlink(model: OutputModel, entityName: String, parentIds: List[UUID], relationName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Map[String, Object]] = {
    relationUpdate(RELATION_OP_UNLINK, model, entityName, parentIds, relationName, payload, session, executor)
  }
  def relationReplace(model: OutputModel, entityName: String, parentIds: List[UUID], relationName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Map[String, Object]] = {
    val unlinkedIds = resolveRelations(model, entityName, List(relationName), AsyncList.fromList(parentIds), session, executor)
    val unlinkedIdsPayloadFuture = unlinkedIds.toList(executor).map(ids => ids.map(write.entityIdPayload))(executor)
    val unlinkResult = unlinkedIdsPayloadFuture.flatMap(unlinkedIdPayload => relationUnlink(model, entityName, parentIds, relationName, unlinkedIdPayload, session, executor))(executor)
    unlinkResult.flatMap(unlinked => {
      // wait until unlink has completed before linking
      val linkResult = relationLink(model, entityName, parentIds, relationName, payload, session, executor)
      linkResult.map(linked => linked ++ unlinked)(executor)
    })(executor)
  }

  def relationChange(model: OutputModel, entityName: String, parentIds: List[UUID], relationName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    // assumes that relationMutation will only return payloads containing one of the following ops: (link), (unlink), (replace), or (link and unlink)
    val emptyDefault = Future.successful(Map.empty[String,Object])
    val payloadMap = payload.asInstanceOf[Map[String,Object]]
    val linked = if(payloadMap.contains(RELATION_OP_LINK)) {
      relationLink(model, entityName, parentIds, relationName, payloadMap(RELATION_OP_LINK), session, executor)
    } else { emptyDefault }
    val unlinked = if(payloadMap.contains(RELATION_OP_UNLINK)) {
      relationUnlink(model, entityName, parentIds, relationName, payloadMap(RELATION_OP_UNLINK), session, executor)
    } else { emptyDefault }
    val replaced = if(payloadMap.contains(RELATION_OP_REPLACE)) {
      relationReplace(model, entityName, parentIds, relationName, payloadMap(RELATION_OP_REPLACE), session, executor)
    } else { emptyDefault }
    implicit val ec: ExecutionContext = executor
    Future.sequence(List(linked, unlinked, replaced)).map(_.reduce(_++_))
  }

  def mutateAndLinkRelations(model: OutputModel, entityName: String, entityIds: List[UUID], payloadMap: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    val relationMutationResults = model.input.entities(entityName).relations.filter(x => payloadMap.contains(x._1)).map((name_relation: (String, RelationField)) => {
      val (relationName, relation) = name_relation
      (relationName, relationMutation(model, relation.targetEntityName, payloadMap(relationName), session, executor))
    })
    val relationLinkResults = relationMutationResults.map((name_result: (String,Future[Object])) => {
      val (relationName, result) = name_result
      result.flatMap(idObjects => relationChange(model, entityName, entityIds, relationName, idObjects, session, executor).map((relationName,_))(executor))(executor)
    })
    implicit val ec: ExecutionContext = executor
    Future.sequence(relationLinkResults).map(relations => {
      entityIds.map(id => {
        relations.toMap ++ write.entityIdPayload(id)
      })
    })
  }

  def createOne(model: OutputModel, entityName: String, payload: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val (uuid, createResults) = write.createEntity(model.entityTables(entityName), payload, session, executor)
    val linkResults = mutateAndLinkRelations(model, entityName, List(uuid), payload, session, executor).map(_.head)(executor)
    implicit val ec: ExecutionContext = executor
    // make resulting future depend on create's insert statements completing successfully
    Future.sequence(createResults.map(_.toList(executor))).flatMap(_ => linkResults)
  }

  // TODO: consider parsing, validating, and converting entire payload before calling any queries/mutations
  // convert scalar arguments from Object to appropriate cassandra type - e.g. a uuid string passed in from json gets converted to java.util.UUID
  def convertPayloadToColumnTypes(payload: Map[String,Object], scalars: Map[String,ScalarField]): Map[String,Object] = {
    payload.map(name_val => (name_val._1, scalars.get(name_val._1).map(_.scalarType.convert(name_val._2)).getOrElse(name_val._2)))
  }
  def convertPayloadToColumnTypes(payloads: List[Map[String,Object]], scalars: Map[String,ScalarField]): List[Map[String,Object]] = {
    payloads.map(convertPayloadToColumnTypes(_, scalars))
  }

  def create(model: OutputModel, entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[List[Map[String, Object]]] = {
    val isList = payload.isInstanceOf[List[Map[String,Object]]]
    val payloadList: List[Map[String,Object]] = if(isList) payload.asInstanceOf[List[Map[String,Object]]] else List(payload.asInstanceOf[Map[String,Object]])
    val convertedPayloads = convertPayloadToColumnTypes(payloadList, model.input.entities(entityName).fields)
    val creates = convertedPayloads.map(createOne(model, entityName, _, session, executor))
    implicit val ec: ExecutionContext = executor
    Future.sequence(creates)
  }

  def matchMutation(model: OutputModel, entityName: String, payload: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    matchEntities(model, entityName, payload, session, executor).toList(executor).map(_.map(id => Map((schema.ENTITY_ID_COLUMN_NAME, id))))(executor)
  }

  def update(model: OutputModel, entityName: String, ids: AsyncList[UUID], payload: Map[String, Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    val results = ids.map(id => {
      val futureMaybeEntity = read.entityIdToObject(model, entityName, id, session, executor)
      futureMaybeEntity.map(_.map(currentEntity => {
        val updateResult = write.updateEntity(model.entityTables(entityName), currentEntity, payload, session, executor)
        val linkResults = mutateAndLinkRelations(model, entityName, List(id), payload, session, executor).map(_.head)(executor)
        updateResult.flatMap(_ => linkResults)(executor)
      }))(executor)
    }, executor)
    AsyncList.unfuture(AsyncList.filterSome(AsyncList.unfuture(results, executor), executor), executor).toList(executor)
  }

  def update(model: OutputModel, entityName: String, payload: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    val convertedPayload = convertPayloadToColumnTypes(payload, model.input.entities(entityName).fields)
    val conditions = convertedPayload(MUTATION_OP_MATCH)
    val ids = matchEntities(model, entityName, conditions, session, executor)
    update(model, entityName, ids, convertedPayload, session, executor)
  }

  def delete(model: OutputModel, entityName: String, ids: AsyncList[UUID], payload: Map[String, Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    implicit val ec: ExecutionContext = executor
    val relations = model.input.entities(entityName).relations
    val results = ids.map(id => {
      val futureMaybeEntity = read.entityIdToObject(model, entityName, id, session, executor)
      futureMaybeEntity.map(_.map(entity => {
        val deleteResult = write.deleteEntity(model.entityTables(entityName),entity, session, executor)
        val childResult = relations.map((name_relation:(String, RelationField)) => {
          val (relationName, relation) = name_relation
          val childIds = resolveRelations(model, entityName, List(relationName), AsyncList.singleton(id), session, executor)
          val unlink = childIds.map(childId => write.deleteBidirectionalRelation(model, entityName, relationName)(session, (id, childId), executor), executor)
          val recurse = if(payload.contains(relationName)) {
            delete(model, relation.targetEntityName, childIds, payload(relationName).asInstanceOf[Map[String, Object]], session, executor).map(Some(relationName, _))
          } else {
            Future.successful(None)
          }
          // make recurse-delete future depend on links being deleted
          AsyncList.unfuture(unlink, executor).toList(executor).flatMap(_ => recurse)
        })
        Future.sequence(childResult).flatMap(relationMapMaybes => deleteResult.map(entityIdMap => entityIdMap ++ relationMapMaybes.filter(_.isDefined).map(_.get).toMap))
      }))(executor)
    }, executor)
    AsyncList.unfuture(AsyncList.filterSome(AsyncList.unfuture(results, executor), executor), executor).toList(executor)
  }

  def delete(model: OutputModel, entityName: String, payload: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    val conditions = payload(MUTATION_OP_MATCH)
    val ids = matchEntities(model, entityName, conditions, session, executor)
    delete(model, entityName, ids, payload, session, executor)
  }


  def mutation(model: OutputModel, entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    if(payload.isInstanceOf[List[Object]]) {
      // if given a list, default to creating a list
      create(model, entityName, payload, session, executor)
    } else if(payload.isInstanceOf[Map[String,Object]]) {
      val payloadMap = payload.asInstanceOf[Map[String,Object]]
      if(payloadMap.contains(MUTATION_OP_CREATE)) {
        create(model, entityName, payloadMap(MUTATION_OP_CREATE), session, executor)
      } else if(payloadMap.contains(MUTATION_OP_MATCH)) {
        matchMutation(model, entityName, payloadMap(MUTATION_OP_MATCH).asInstanceOf[Map[String,Object]], session, executor)
      } else if(payloadMap.contains(MUTATION_OP_UPDATE)) {
        update(model, entityName, payloadMap(MUTATION_OP_UPDATE).asInstanceOf[Map[String,Object]], session, executor)
      } else if(payloadMap.contains(MUTATION_OP_DELETE)) {
        delete(model, entityName, payloadMap(MUTATION_OP_DELETE).asInstanceOf[Map[String,Object]], session, executor)
      } else {
        // default to create
        create(model, entityName, payloadMap, session, executor)
      }
    } else {
      Future.failed(new RuntimeException(s"attempted to mutate entity of type '${entityName}' but payload has type '${payload.getClass}', must be either Map or List"))
    }
  }

  def linkMutation(model: OutputModel, entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Object] = {
    mutation(model, entityName, payload, session, executor).map(x => Map((RELATION_OP_LINK, x)))(executor)
  }
  def unlinkMutation(model: OutputModel, entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Object] = {
    // TODO: check for valid op types - only match, and maybe delete?
    mutation(model, entityName, payload, session, executor).map(x => Map((RELATION_OP_UNLINK, x)))(executor)
  }
  def replaceMutation(model: OutputModel, entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Object] = {
    mutation(model, entityName, payload, session, executor).map(x => Map((RELATION_OP_REPLACE, x)))(executor)
  }
  def relationMutation(model: OutputModel, entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[Object] = {
    if(payload.isInstanceOf[List[Object]]) {
      // if given a list, default to replacing with list of created
      replaceMutation(model, entityName, payload, session, executor)
    } else if(payload.isInstanceOf[Map[String,Object]]) {
      val payloadMap = payload.asInstanceOf[Map[String,Object]]
      // TODO: change else-if conditions to allow both link and unlink, but not replace at the same time
      if(payloadMap.contains(RELATION_OP_LINK)) {
        linkMutation(model, entityName, payloadMap(RELATION_OP_LINK), session, executor)
      } else if(payloadMap.contains(RELATION_OP_UNLINK)) {
        unlinkMutation(model, entityName, payloadMap(RELATION_OP_UNLINK), session, executor)
      } else if(payloadMap.contains(RELATION_OP_REPLACE)) {
        replaceMutation(model, entityName, payloadMap(RELATION_OP_REPLACE), session, executor)
      } else {
        // default to replacing with single mutation
        replaceMutation(model, entityName, payload, session, executor)
      }
    } else {
      Future.failed(new RuntimeException(s"attempted to mutate entity of type '${entityName}' but payload has type '${payload.getClass}', must be either Map or List"))
    }
  }


}