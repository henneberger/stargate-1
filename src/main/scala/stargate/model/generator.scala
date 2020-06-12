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

package stargate.model

import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import stargate.model.queries.{GetQuery, GetSelection}

import scala.util.Random
import stargate.schema.{RELATION_JOIN_STRING, RELATION_SPLIT_REGEX}
import stargate.util.AsyncList
import stargate.{keywords, schema, util}

import scala.concurrent.{ExecutionContext, Future}

object generator {


  def randomScalar(`type`: ScalarType.Value): Object = {
    `type` match {
      case ScalarType.BOOLEAN => java.lang.Boolean.valueOf(Random.nextBoolean())
      case ScalarType.INT => Random.nextInt.asInstanceOf[Object]
      case ScalarType.FLOAT => Random.nextFloat.asInstanceOf[Object]
      case ScalarType.STRING => Random.alphanumeric.take(10).mkString.asInstanceOf[Object]
      case ScalarType.UUID => UUID.randomUUID.asInstanceOf[Object]
      case _ => throw new UnsupportedOperationException(s"cannot generate random value for type scalar: ${`type`.name}")
    }
  }

  def randomScalar(field: ScalarField): Object = randomScalar(field.scalarType)


  def entityFields(fields: List[ScalarField]): Map[String, Object] = {
    fields.map(f => (f.name, randomScalar(f))).filter(_._1 != stargate.schema.ENTITY_ID_COLUMN_NAME).toMap
  }

  private def createEntities(model: Entities, visitedEntities: Set[String], entityName: String, remaining: Int, maxBranching: Int, allowInverse: Boolean = false): (Int, Map[String, Object]) = {
    val entity = model(entityName)
    val scalars = entityFields(entity.fields.values.toList)
    val (nextRemaining, result) = entity.relations.values.foldLeft((remaining - 1, scalars))((remaining_results, relation) => {
      val (remaining, results) = remaining_results
      if (remaining <= 0 || (visitedEntities(relation.targetEntityName) && !allowInverse)) {
        (remaining, results)
      } else {
        val numChildren = Random.nextInt(Math.min(remaining, maxBranching))
        val (nextRemaining, children) = List.range(0, numChildren).foldLeft((remaining, List.empty[Map[String, Object]]))((remaining_children, _) => {
          val (remaining, children) = remaining_children
          if (remaining <= 0) {
            (remaining, children)
          } else {
            val (size, recurse) = createEntities(model, Set(relation.targetEntityName) ++ visitedEntities, relation.targetEntityName, remaining, maxBranching, allowInverse)
            (remaining - size, List(recurse) ++ children)
          }
        })
        (nextRemaining, results ++ Map((relation.name, children)))
      }
    })
    (remaining - nextRemaining, result)
  }

  def createEntity(model: Entities, entityName: String, remaining: Int = 50, maxBranching: Int = 5, allowInverse: Boolean = false): Map[String, Object] = {
    createEntities(model, Set(entityName), entityName, remaining, maxBranching, allowInverse)._2
  }


  def resolveEntityValue(entity: Map[String, Object], path: List[String], executor: ExecutionContext): Future[Option[Object]] = {
    val (head :: _, tail) = path.splitAt(1)
    if(tail.isEmpty) {
      Future.successful(entity.get(head))
    } else {
      val children = entity.get(head).map(_.asInstanceOf[AsyncList[Map[String,Object]]]).getOrElse(AsyncList.empty)
      children.take(1, executor).flatMap(child => {
        if(child.isEmpty) {
          Future.successful(None)
        } else {
          resolveEntityValue(child.head, tail, executor)
        }
      })(executor)
    }
  }

  def randomMatchCondition(model: Entities, entityName: String, maxTerms: Int): List[ScalarCondition[Object]] = {
    val entity = model(entityName)
    val relatedEntities = entity.relations.view.mapValues(r => model(r.targetEntityName))
    val scalars = entity.fields.values ++ relatedEntities.toList.flatMap(re => re._2.fields.values.map(f => f.rename(re._1 + RELATION_JOIN_STRING + f.name)))
    val shuffled: List[ScalarField] = Random.shuffle(scalars.toList)
    shuffled.take(Random.between(1, maxTerms + 1)).map(f => ScalarCondition(f.name, ScalarComparison.EQ, randomScalar(f)))
  }
  def specificMatchCondition(model: OutputModel, entityName: String, maxTerms: Int, session: CqlSession, executor: ExecutionContext): Future[List[ScalarCondition[Object]]] = {
    val entity = model.input.entities(entityName)
    val query = GetQuery(schema.MATCH_ALL_CONDITION, GetSelection(entity.relations.view.mapValues(_ => GetSelection.empty).toMap, None, None, continue = false, None))
    val entities = stargate.query.get(stargate.query.Context(model, session, executor), entityName, query)
    val random = randomMatchCondition(model.input.entities, entityName, maxTerms)
    entities.take(1, executor).flatMap(entities => {
      if(entities.isEmpty) {
        Future.successful(List.empty)
      } else {
        val entity = entities.head
        val specific = random.map(cond => {
          val path = cond.field.split(RELATION_SPLIT_REGEX).toList
          resolveEntityValue(entity, path, executor).map(_.map(cond.replaceArgument))(executor)
        })
        util.sequence(specific, executor).map(conds => {
          val definedConds = conds.filter(_.isDefined)
          val maybeConds = Option.when(definedConds.nonEmpty)(definedConds.map(_.get))
          maybeConds.getOrElse(List.empty)
        })(executor)
      }
    })(executor)
  }
  private def untypedCondition(conds: List[ScalarCondition[Object]]): Object = {
    if(conds.nonEmpty) conds.flatMap(cond => List(cond.field, cond.comparison.toString, cond.argument)) else keywords.query.MATCH_ALL
  }


  private def getSelection(model: Entities, visitedEntities: Set[String], entityName: String, limit: Int): Map[String,Object] = {
    val entity = model(entityName)
    val nextVisited: Set[String] = visitedEntities ++ entity.relations.values.map(_.targetEntityName)
    val children = entity.relations.view.filter(r => !visitedEntities(r._2.targetEntityName)).mapValues(r => getSelection(model, nextVisited, r.targetEntityName, limit)).toMap[String,Object]
    val flags = Map[String,Object]((keywords.pagination.LIMIT, Integer.valueOf(limit)), (keywords.pagination.CONTINUE, java.lang.Boolean.FALSE))
    flags ++ children
  }
  def randomGetRequest(model: Entities, entityName: String, limit: Int): Map[String, Object] = {
    val condition = randomMatchCondition(model, entityName, 4)
    val selection = getSelection(model, Set(entityName), entityName, limit)
    selection.updated(keywords.mutation.MATCH, untypedCondition(condition))
  }
  def specificGetRequest(model: OutputModel, entityName: String, limit: Int, session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomized = randomGetRequest(model.input.entities, entityName, limit)
    val conditions = specificMatchCondition(model, entityName, 4, session, executor)
    conditions.map(conditions => randomized.updated(keywords.mutation.MATCH, untypedCondition(conditions)))(executor)
  }



  private def randomCreateRequest(makeConditions: String => Future[List[ScalarCondition[Object]]], model: Entities, entityName: String, visitedEntities: Set[String], executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    val entity = model(entityName)
    val size = if (Random.nextDouble() < 0.8) 1 else 2
    util.sequence(List.range(0, size).map(_ => {
      val scalars = entityFields(entity.fields.values.toList)
      val selectedRelations = entity.relations.values.filter(r => !visitedEntities.contains(r.targetEntityName)).filter(_ => Random.nextDouble() < 0.7)
      val nextVisited = visitedEntities ++ selectedRelations.map(_.targetEntityName)
      val relations = util.sequence(selectedRelations.map(r => {
        randomMutationRequest(makeConditions, model, r.targetEntityName, nextVisited, executor).map(req => (r.name, req))(executor)
      }).toList, executor)
      relations.map(relations => relations.toMap ++ scalars)(executor)
    }), executor)
  }
  private def randomUpdateRequest(makeConditions: String => Future[List[ScalarCondition[Object]]], model: Entities, entityName: String, visitedEntities: Set[String], executor: ExecutionContext): Future[Map[String,Object]] = {
    val entity = model(entityName)
    val scalarChanges = entityFields(entity.fields.values.toList).filter(_ => Random.nextDouble() < 0.7)
    val selectedRelations = entity.relations.values.filter(r => !visitedEntities.contains(r.targetEntityName)).filter(_ => Random.nextDouble() < 0.7)
    val nextVisited = visitedEntities ++ selectedRelations.map(_.targetEntityName)
    val relations = util.sequence(selectedRelations.map(r => {
      randomLinkMutationRequest(makeConditions, model, r.targetEntityName, nextVisited, executor).map(req => (r.name, req))(executor)
    }).toList, executor)
    val conditions = makeConditions(entityName)
    conditions.flatMap(conditions => relations.map(relations => scalarChanges ++ relations.toMap ++ Map((keywords.mutation.MATCH, untypedCondition(conditions))))(executor))(executor)
  }
  private def randomMutationRequest(makeConditions: String => Future[List[ScalarCondition[Object]]], model: Entities, entityName: String, visitedEntities: Set[String], executor: ExecutionContext): Future[Map[String,Object]] = {
    val op = Random.nextInt(2)
    if(op == 0) {
      randomCreateRequest(makeConditions, model, entityName, visitedEntities, executor).map(req => Map((keywords.mutation.CREATE, req)))(executor)
    } else if(op == 1) {
      randomUpdateRequest(makeConditions, model, entityName, visitedEntities, executor).map(req => Map((keywords.mutation.UPDATE, req)))(executor)
    } else
      throw new RuntimeException
  }
  private def randomLinkMutationRequest(makeConditions: String => Future[List[ScalarCondition[Object]]], model: Entities, entityName: String, visitedEntities: Set[String], executor: ExecutionContext): Future[Map[String,Object]] = {
    val op = Random.nextInt(3)
    if(op == 0) {
      randomMutationRequest(makeConditions, model, entityName, visitedEntities, executor).map(req => Map((keywords.relation.LINK, req)))(executor)
    } else if(op == 1) {
      makeConditions(entityName).map(conds => Map((keywords.relation.UNLINK, untypedCondition(conds))))(executor)
    } else if(op == 2) {
      randomMutationRequest(makeConditions, model, entityName, visitedEntities, executor).map(req => Map((keywords.relation.REPLACE, req)))(executor)
    } else
      throw new RuntimeException
  }

  def randomCreateRequest(model: Entities, entityName: String): List[Map[String,Object]] = {
    util.await(randomCreateRequest(e => Future.successful(randomMatchCondition(model, e, 4)), model, entityName, Set(entityName), ExecutionContext.parasitic)).get
  }
  def randomUpdateRequest(model: Entities, entityName: String): Map[String,Object] = {
    util.await(randomUpdateRequest(e => Future.successful(randomMatchCondition(model, e, 4)), model, entityName, Set(entityName), ExecutionContext.parasitic)).get
  }
  def specificCreateRequest(model: OutputModel, entityName: String, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    randomCreateRequest(e => specificMatchCondition(model, e, 4, session, executor), model.input.entities, entityName, Set(entityName), executor)
  }
  def specificUpdateRequest(model: OutputModel, entityName: String, session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    randomUpdateRequest(e => specificMatchCondition(model, e, 4, session, executor), model.input.entities, entityName, Set(entityName), executor)
  }

  private def deleteSelection(model: Entities, visitedEntities: Set[String], entityName: String): Map[String,Object] = {
    val entity = model(entityName)
    val selectedRelations = entity.relations.values.filter(r => !visitedEntities(r.targetEntityName)).filter(_ => Random.nextDouble() < 0.1)
    val nextVisited: Set[String] = visitedEntities ++ selectedRelations.map(_.targetEntityName)
    val children = selectedRelations.map(r => (r.name, deleteSelection(model, nextVisited, r.targetEntityName))).toMap[String,Object]
    children
  }
  def randomDeleteRequest(model: Entities, entityName: String): Map[String, Object] = {
    val condition = randomMatchCondition(model, entityName, 4)
    val selection = deleteSelection(model, Set(entityName), entityName)
    selection.updated(keywords.mutation.MATCH, untypedCondition(condition))
  }
  def specificDeleteRequest(model: OutputModel, entityName: String, session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomized = randomDeleteRequest(model.input.entities, entityName)
    val conditions = specificMatchCondition(model, entityName, 4, session, executor)
    conditions.map(conditions => randomized.updated(keywords.mutation.MATCH, untypedCondition(conditions)))(executor)
  }

}