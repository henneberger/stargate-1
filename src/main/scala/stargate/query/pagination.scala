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

package stargate.query

import java.util.UUID

import stargate.model.InputModel
import stargate.model.queries.GetSelection
import stargate.keywords
import stargate.util
import stargate.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}

object pagination {

  // maps uuid to stream of entities, as well as the TTL and get request on that stream
  case class StreamEntry(entityName: String, getRequest: GetSelection, ttl: Int, entities: AsyncList[Map[String,Object]])
  type Streams = Map[UUID, StreamEntry]
  type TruncateResult = Future[(List[Map[String,Object]], Map[String,Object], Streams)]


  // wraps eager reads (tree of Lists) to look like lazy reads (tree of AsyncLists)
  def untruncate(model: InputModel, entityName: String, entities: List[Map[String,Object]]): AsyncList[Map[String,Object]] = {
    val relations = model.entities(entityName).relations
    val updatedEntities = entities.map(entity => {
      val includedRelations = entity.toList.filter(kv => relations.contains(kv._1))
      entity ++ includedRelations.map(kv => (kv._1, untruncate(model, relations(kv._1).targetEntityName, kv._2.asInstanceOf[List[Map[String,Object]]])))
    })
    AsyncList.fromList(updatedEntities)
  }

  // given a tree of entities in (lazy) AsyncLists, chop off the first N entities, and return a uuid pointing to the rest of the list
  def truncate(model: InputModel, entityName: String, getRequest: GetSelection,
               entities: AsyncList[Map[String,Object]], defaultLimit: Integer, defaultTTL: Integer, executor: ExecutionContext): TruncateResult = {
    truncate(model, entityName, getRequest, entities, UUID.randomUUID, defaultLimit, defaultTTL, executor)
  }

  def truncate(model: InputModel, entityName: String, getRequest: GetSelection,
               entities: AsyncList[Map[String,Object]], streamId: UUID, defaultLimit: Int, defaultTTL: Int, executor: ExecutionContext): TruncateResult = {

    val limit = getRequest.limit.getOrElse(defaultLimit)
    val continue = getRequest.continue
    val ttl = getRequest.ttl.getOrElse(defaultTTL)

    val (head, tail) = entities.splitAt(limit, executor)

    val updatedEntities = util.flattenFLF(head.map(entities => {
      entities.map(entity => truncateRelations(model, entityName, getRequest.relations, entity, defaultLimit, defaultTTL, executor))
    })(executor), executor)

    val merged = updatedEntities.map(entities_metadatas_streams => {
      val (entities, metadata, streams) = entities_metadatas_streams.unzip3
      val emptyMetadataStream: (Map[String,Object], Streams) = (Map.empty, Map.empty)
      val baseMetadataStream: Future[(Map[String,Object], Streams)] = if(continue) {
        val continueMetadata = Map[String,Object]((keywords.response.continue.CONTINUE_ID, streamId))
        val continueStream: Streams = Map((streamId, StreamEntry(entityName, getRequest, ttl, tail)))
        tail.isEmpty(executor).map(isEmpty => { if(!isEmpty) (continueMetadata, continueStream) else emptyMetadataStream })(executor)
      } else {
        Future.successful(emptyMetadataStream)
      }
      val zippedMetadata = entities.map(_(stargate.schema.ENTITY_ID_COLUMN_NAME)).zip(metadata).toMap
      val mergedMetadataStreams = baseMetadataStream.map(base => {
        val mergedMetadata = base._1 ++ Map[String,Object]((keywords.response.continue.ENTITIES, zippedMetadata))
        val mergedStreams = streams.flatten.toMap ++ base._2
        (mergedMetadata, mergedStreams)
      })(executor)
      (entities, mergedMetadataStreams)
    })(executor)

    merged.flatMap(merged => merged._2.map(metadata_streams => (merged._1, metadata_streams._1, metadata_streams._2))(executor))(executor)
  }

  // for one entity and a selection of relations (and their corresponding get requests), truncate all children and return their streams
  def truncateRelations(model: InputModel, entityName: String, relationRequests: Map[String, GetSelection],
               entity: Map[String,Object], defaultLimit: Integer, defaultTTL: Integer, executor: ExecutionContext): Future[(Map[String,Object], Map[String,Object], Streams)] = {
    implicit val ec: ExecutionContext = executor
    val modelEntity = model.entities(entityName)
    val childResults = relationRequests.toList.map(name_relation_request => {
      val (relationName, request) = name_relation_request
      val relation = modelEntity.relations(relationName)
      val recurse = truncate(model, relation.targetEntityName, request,
        entity(relationName).asInstanceOf[AsyncList[Map[String,Object]]], defaultLimit, defaultTTL, executor)
      recurse.map((relationName, _))
    })
    Future.sequence(childResults).map(childResults => {
      childResults.foldLeft((entity, Map.empty[String,Object], Map.empty : Streams))((entity_metadata_streams, childResult) => {
        val (entity, metadata, streams) = entity_metadata_streams
        (entity.updated(childResult._1, childResult._2._1), metadata.updated(childResult._1, childResult._2._2), streams ++ childResult._2._3)
      })
    })
  }


}