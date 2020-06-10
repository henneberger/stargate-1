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

package stargate.model.queries

import stargate.model.{ScalarComparison, _}
import stargate.schema.{ENTITY_ID_COLUMN_NAME, GroupedConditions}
import stargate.{keywords, schema}

import scala.util.Try

object parser {

  def contextMessage(context: List[String]): String = {
    if(context.isEmpty) {
      "in root context"
    } else {
      s"""in context: ${context.reverse.mkString(".")}"""
    }
  }

  def checkUnusedFields(context: List[String], unusedKeys: Set[String], allowedKeywords: Set[String], allowedFields: Set[String]) = {
    val (unusedKeywords, unusedFields) = unusedKeys.partition(_.startsWith(keywords.KEYWORD_PREFIX))
    require(unusedKeywords.isEmpty, s"Found keywords: ${unusedKeywords}, only the following are allowed: ${allowedKeywords}, ${contextMessage(context)}")
    require(unusedFields.isEmpty, s"Found unused fields: ${unusedFields}, only the following are allowed: ${allowedFields}, ${contextMessage(context)}")
  }
  def cast[T](context: List[String], value: Object, toType: Class[T]): T = {
    val cast = Try(toType.cast(value))
    require(cast.isSuccess, s"Failed to cast type ${contextMessage(context)}, expected ${toType}, but got ${value.getClass}")
    cast.get
  }

  // no return, since no transformation is performed
  def parseGetSelection(entities: Entities, context: List[String], entityName: String, payload: Map[String,Object]): GetSelection = {
    import keywords.query.INCLUDE
    import keywords.pagination._
    val entity = entities(entityName)
    val allowedKeywords = Set(INCLUDE, LIMIT, CONTINUE, TTL)
    val unusedKeys = payload.keySet.diff(entity.relations.keySet ++ allowedKeywords)
    checkUnusedFields(context, unusedKeys, allowedKeywords, entity.relations.keySet)
    val include: Option[List[String]] = payload.get(INCLUDE).map(cast(INCLUDE +: context, _, classOf[List[String]]))
    include.foreach(_.foreach(field => require(entity.fields.contains(field), s"Entity ${entityName} does not have a field ${field} ${contextMessage(context)}")))
    include.foreach(fields => require(fields.contains(schema.ENTITY_ID_COLUMN_NAME), s"Included fields ${fields} for entity ${entityName} must contain ${schema.ENTITY_ID_COLUMN_NAME} ${contextMessage(context)}"))
    val relations = entity.relations.values.filter(r => payload.contains(r.name)).map(r =>
      (r.name, parseGetSelection(entities, r.name +: context, r.targetEntityName, cast(r.name +: context, payload(r.name), classOf[Map[String,Object]]))))
    val limit: Option[Int] = payload.get(LIMIT).map(cast(LIMIT +: context, _, classOf[java.lang.Integer]))
    val continue: java.lang.Boolean = payload.get(CONTINUE).exists(cast(CONTINUE +: context, _, classOf[java.lang.Boolean]))
    val ttl: Option[Int] = payload.get(TTL).map(cast(TTL +: context, _, classOf[java.lang.Integer]))
    GetSelection(relations.toMap, include, limit, continue, ttl)
  }
  def parseGet(entities: Entities, entityName: String, payload: Map[String,Object]): GetQuery = {
    val conditions = parseConditions(entities, List.empty, entityName, payload(keywords.mutation.MATCH))
    val selection = parseGetSelection(entities, List.empty, entityName, payload.removed(keywords.mutation.MATCH))
    GetQuery(conditions, selection)
  }

  def parseEntity(entities: Entities, context: List[String], entityName: String, payload: Map[String, Object], allowedKeywords: Set[String]): (Map[String,Object], Map[String, RelationMutation]) = {
    require(!payload.contains(ENTITY_ID_COLUMN_NAME), s"mutation may not specify field: ${ENTITY_ID_COLUMN_NAME}, ${contextMessage(context)}")
    val entity = entities(entityName)
    val allowedFields = entity.fields.keySet ++ entity.relations.keySet
    val unusedKeys = payload.keySet.diff(allowedFields ++ allowedKeywords)
    checkUnusedFields(context, unusedKeys, allowedKeywords, allowedFields)
    val updatedScalars = entity.fields.values.filter(f => payload.contains(f.name)).map(field => {
      val converted = Try(field.scalarType.convert(payload(field.name)))
      require(converted.isSuccess, s"Field type check failed ${contextMessage(field.name +: context)}: ${converted.failed.get}")
      (field.name, converted.get)
    })
    val updatedRelations = entity.relations.values.filter(r => payload.contains(r.name)).map(relation => {
      val nextContext = relation.name +: context
      (relation.name, parseLinkMutation(entities, nextContext, relation.targetEntityName, cast(nextContext, payload(relation.name), classOf[Map[String,Object]])))
    })
    (updatedScalars.toMap, updatedRelations.toMap)
  }

  def parseCreateOne(entities: Entities, context: List[String], entityName: String, payload: Map[String, Object]): CreateOneMutation = {
    val entity = entities(entityName)
    val wrappedRelationsPayload: Map[String,Object] = payload.map(kv => {
      val wrapped = if(entity.relations.contains(kv._1)) { Map((keywords.relation.LINK, kv._2)) } else { kv._2 }
      (kv._1, wrapped)
    })
    val (fields, relations) = parseEntity(entities, context, entityName, wrappedRelationsPayload, Set.empty)
    CreateOneMutation(fields, relations.view.mapValues(v => v.asInstanceOf[LinkMutation].mutation).toMap)
  }

  def parseCreate(entities: Entities, context: List[String], entityName: String, payload: Object): CreateMutation = {
    val createOnes = if (payload.isInstanceOf[List[Map[String, Object]]]) {
      val payloadList = payload.asInstanceOf[List[Map[String, Object]]]
      val indexedPayloads = List.range(0, payloadList.length).zip(payloadList)
      indexedPayloads.map(ic => parseCreateOne(entities, ic._1.toString +: context, entityName, ic._2))
    } else if (payload.isInstanceOf[Map[String, Object]]) {
      List(parseCreateOne(entities, "0" +: context, entityName, payload.asInstanceOf[Map[String, Object]]))
    } else {
      throw new RuntimeException(s"create request must be either a Map or List[Map], instead got ${payload.getClass}")
    }
    CreateMutation(createOnes)
  }
  def parseCreate(entities: Entities, entityName: String, payload: Object): CreateMutation = parseCreate(entities, List.empty, entityName, payload)


  def parseConditions[T](validateArgument: (List[String], ScalarField, ScalarComparison.Value, Object) => T, entities: Entities, context:List[String], entityName: String, payload: Object): GroupedConditions[T] = {
    def parseCondition(field_op_val: List[Object]): ScalarCondition[Object] = {
      require(field_op_val.length == 3, s"condition must have exactly 3 elements (field, comparison, argument) ${contextMessage(context)}")
      val field :: comparison :: value :: _ = field_op_val
      val parsedComparison = Try(ScalarComparison.fromString(comparison.toString))
      require(parsedComparison.isSuccess, s"Found invalid comparison operator: ${comparison} ${contextMessage(context)}, valid options are: ${ScalarComparison.names.keySet}")
      ScalarCondition(cast(field.toString +: context, field, classOf[String]), parsedComparison.get, value)
    }
    if(payload == keywords.query.MATCH_ALL) {
      schema.MATCH_ALL_CONDITION
    } else if(payload.isInstanceOf[List[Object]]) {
      val payloadList = payload.asInstanceOf[List[Object]]
      require(payloadList.nonEmpty, s"""match conditions must be either a non-empty list of triples, or the keyword "${keywords.query.MATCH_ALL}" ${contextMessage(context)}""")
      val conditions = payloadList.grouped(3).map(parseCondition).toList
      val groupedConditions = schema.groupConditionsByPath(conditions)
      groupedConditions.map(path_conds => {
        val (path, conditions) = path_conds
        val targetEntity = Try(entities(schema.traverseEntityPath(entities, entityName, path)))
        require(targetEntity.isSuccess, s"${path.mkString(".")} does not represent a valid scalar for entity ${entityName} ${contextMessage(context)}")
        val validatedConditions = conditions.map(condition => {
          val targetField = Try(targetEntity.get.fields(condition.field))
          require(targetField.isSuccess, s"Field ${condition.field} does not represent a valid scalar for entity ${targetEntity.get.name} ${contextMessage(context)}")
          condition.replaceArgument(validateArgument(context, targetField.get, condition.comparison, condition.argument))
        })
        (path, validatedConditions)
      })
    } else {
      throw new RuntimeException(
        s"""conditions must be either a non-empty List[Object] of (field, comparison, argument) triples,
        | or the string "${keywords.query.MATCH_ALL}", instead got ${payload.getClass} ${contextMessage(context)}""".stripMargin)
    }
  }
  def parseConditions(entities: Entities, context:List[String], entityName: String, payload: Object): GroupedConditions[Object] = {
    def convert(context:List[String], field: ScalarField, comparison: ScalarComparison.Value, arg: Object): Object = {
      def convertInner(arg: Object): Object = {
        val converted = Try(field.scalarType.convert(arg))
        require(converted.isSuccess, s"Failed to convert field ${field.name} to type ${field.scalarType.name} from type ${arg.getClass} ${contextMessage(context)}")
        converted.get
      }
      if(comparison == ScalarComparison.IN) {
        val asList = Try(arg.asInstanceOf[List[Object]])
        require(asList.isSuccess, s"Argument ${arg} for ${ScalarComparison.IN.toString} condition must be a list ${contextMessage(context)}")
        asList.get.map(convertInner)
      } else {
        convertInner(arg)
      }
    }
    parseConditions(convert, entities, context, entityName, payload)
  }
  def parseNamedConditions(entities: Entities, context:List[String], entityName: String, payload: Object): GroupedConditions[(ScalarType.Value, String)] = {
    def convert(context:List[String], field: ScalarField, comparison: ScalarComparison.Value, arg: Object): (ScalarType.Value, String) = (field.scalarType, cast(field.name +: context, arg, classOf[String]))
    parseConditions(convert, entities, context, entityName, payload)
  }

  def parseUpdate(entities: Entities, context:List[String], entityName: String, payload: Map[String, Object]): UpdateMutation = {
    import keywords.mutation.MATCH
    require(payload.contains(MATCH), s"""Update request must contain keyword "${MATCH}" to specify match conditions ${contextMessage(context)}""")
    val conditions = parseConditions(entities, MATCH +: context, entityName, payload(keywords.mutation.MATCH))
    val (fields, relations) = parseEntity(entities, context, entityName, payload.removed(keywords.mutation.MATCH), Set.empty)
    UpdateMutation(conditions, fields, relations)
  }
  def parseUpdate(entities: Entities, entityName: String, payload: Map[String, Object]): UpdateMutation = parseUpdate(entities, List.empty, entityName, payload)

  def parseMutation(entities: Entities, context:List[String], entityName: String, payload: Object): Mutation = {
    if (payload.isInstanceOf[List[Object]]) {
      // assume create if payload is list
      parseCreate(entities, context, entityName, payload)
    } else if (payload.isInstanceOf[Map[String, Object]]) {
      val payloadMap = payload.asInstanceOf[Map[String, Object]]
      if (payloadMap.keySet == Set(keywords.mutation.CREATE)) {
        parseCreate(entities, context, entityName, payloadMap(keywords.mutation.CREATE))
      } else if (payloadMap.keySet == Set(keywords.mutation.MATCH)) {
        MatchMutation(parseConditions(entities, context, entityName, payloadMap(keywords.mutation.MATCH)))
      } else if (payloadMap.keySet == Set(keywords.mutation.UPDATE)) {
        parseUpdate(entities, context, entityName, payloadMap(keywords.mutation.UPDATE).asInstanceOf[Map[String,Object]])
      } else {
        // default to create
        parseCreate(entities, context, entityName, payload)
      }
    } else {
      throw new RuntimeException(
        s"mutation request must be either a Map or List[Map], instead got ${payload.getClass}")
    }
  }

  def parseLinkMutation(entities: Entities, context: List[String], entityName: String, payload: Map[String,Object]): RelationMutation = {
    if (payload.isInstanceOf[List[Object]]) {
      // if list, assume replace-create
      ReplaceMutation(parseMutation(entities, context, entityName, payload))
    } else if (payload.isInstanceOf[Map[String, Object]]) {
      val payloadMap = payload.asInstanceOf[Map[String, Object]]
      if(payload.keySet == Set(keywords.relation.LINK)) {
        LinkMutation(parseMutation(entities, context, entityName, payloadMap(keywords.relation.LINK)))
      } else if(payload.keySet == Set(keywords.relation.UNLINK)) {
        UnlinkMutation(parseConditions(entities, context, entityName, payloadMap(keywords.relation.UNLINK)))
      } else if(payload.keySet == Set(keywords.relation.REPLACE)) {
        ReplaceMutation(parseMutation(entities, context, entityName, payloadMap(keywords.relation.REPLACE)))
      } else {
        // assume replace if not specified
        ReplaceMutation(parseMutation(entities, context, entityName, payloadMap))
      }
    } else {
      throw new RuntimeException(
        s"nested mutation request must be either a Map or List[Map], instead got ${payload.getClass}")
    }
  }

  def parseDeleteSelection(entities: Entities, context:List[String], entityName: String, payload: Map[String,Object]): DeleteSelection = {
    val entity = entities(entityName)
    val unusedKeys = payload.keySet.diff(entity.relations.keySet)
    checkUnusedFields(context, unusedKeys, Set.empty, entity.relations.keySet)
    val relations = entity.relations.values.filter(r => payload.contains(r.name)).map(r => {
      (r.name, parseDeleteSelection(entities, r.name +: context, r.targetEntityName, cast(r.name +: context, payload(r.name), classOf[Map[String,Object]])))
    })
    DeleteSelection(relations.toMap)
  }
  def parseDelete(entities: Entities, entityName: String, payload: Map[String,Object]): DeleteQuery = {
    val conditions = parseConditions(entities, List.empty, entityName, payload(keywords.mutation.MATCH))
    val selection = parseDeleteSelection(entities, List.empty, entityName, payload.removed(keywords.mutation.MATCH))
    DeleteQuery(conditions, selection)
  }

}
