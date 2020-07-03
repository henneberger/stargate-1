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

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.delete.Delete
import com.datastax.oss.driver.api.querybuilder.insert.{Insert, RegularInsert}
import com.datastax.oss.driver.internal.core.util.Strings
import stargate.cassandra.CassandraTable
import stargate.model.{OutputModel, ScalarComparison, ScalarCondition}
import stargate.schema


// functions used to implement stargate mutations
object write {

  def insertStatement(keyspace: String, tableName: String, columns: Map[String, Object]): Insert = {
    val base = QueryBuilder.insertInto(Strings.doubleQuote(keyspace), Strings.doubleQuote(tableName)).asInstanceOf[RegularInsert]
    columns.foldLeft(base)((builder, col) => builder.value(Strings.doubleQuote(col._1), QueryBuilder.literal(col._2)))
  }

  def insertStatement(table: CassandraTable, columns: Map[String, Object]): Insert = {
    insertStatement(table.keyspace, table.name, table.columns.fullKeyPhysicalValues(columns))
  }

  def deleteStatement(keyspace: String, tableName: String, conditions: List[ScalarCondition[Object]]): Delete = {
    val base = QueryBuilder.deleteFrom(Strings.doubleQuote(keyspace), Strings.doubleQuote(tableName)).where()
    read.appendWhere(base, conditions)
  }

  def deleteStatement(keyspace: String, tableName: String, conditions: Map[String,Object]): Delete = {
    deleteStatement(keyspace, tableName, conditions.toList.map(key_val => ScalarCondition(key_val._1, ScalarComparison.EQ, key_val._2)))
  }

  def deleteStatement(table: CassandraTable, conditions: List[ScalarCondition[Object]]): Delete = {
    deleteStatement(table.keyspace, table.name, read.physicalConditions(table.columns.combinedMap, conditions))
  }

  def deleteEntityStatement(table: CassandraTable, columns: Map[String, Object]): Delete = {
    deleteStatement(table.keyspace, table.name, table.columns.key.fullKeyPhysicalValues(columns))
  }

  def entityIdPayload(entityId: UUID): Map[String,UUID] = Map((schema.ENTITY_ID_COLUMN_NAME, entityId))
  def entityIdPayload(entity: Map[String,Object]): Map[String,UUID] = entityIdPayload(entity(schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])


  def createEntity(tables: List[CassandraTable], payload: Map[String,Object]): (UUID, List[SimpleStatement]) = {
    // TODO - could use cassandra-generated timeuuid instead of random, but then the first write would block all the secondary writes
    val uuid = UUID.randomUUID()
    val payloadMap = payload.updated(schema.ENTITY_ID_COLUMN_NAME, uuid)
    val inserts = tables.map(t => insertStatement(t, payloadMap)).map(_.build)
    (uuid, inserts)
  }

  def deleteEntity(tables: List[CassandraTable], entity: Map[String,Object]): List[SimpleStatement] = {
    val deleteResults = tables.map(table => deleteEntityStatement(table, entity))
    deleteResults.map(_.build)
  }

  def updateEntity(tables: List[CassandraTable], currentEntity: Map[String,Object], changes: Map[String,Object]): List[SimpleStatement] = {
    val updateStatements = tables.map(table => {
      // TODO: allow changes to contain null for key values
      val keyChanged = table.columns.key.combined.exists(col => changes.get(col.name).orNull != null)
      if(keyChanged) {
        val maybeDelete = write.deleteEntityStatement(table, currentEntity)
        val insertPayload = (currentEntity++changes)
        val maybeInsert = write.insertStatement(table, insertPayload)
        List(maybeDelete, maybeInsert).map(_.build)
      } else {
        val keyColumns = table.columns.key.names.combined.toSet
        val dataColumns = table.columns.data.map(_.name).toSet
        val insertPayload = currentEntity.filter(kv => keyColumns.contains(kv._1)) ++ changes.filter(kv => dataColumns.contains(kv._1))
        val maybeInsert = write.insertStatement(table, insertPayload)
        List(maybeInsert.build)
      }
    })
    updateStatements.flatten
  }

  def relationColumnValues(from: UUID, to: UUID): Map[String, UUID] = Map((schema.RELATION_FROM_COLUMN_NAME, from),(schema.RELATION_TO_COLUMN_NAME, to))
  def createDirectedRelationStatement(keyspace: String, tableName: String, from: UUID, to: UUID): SimpleStatement = insertStatement(keyspace, tableName, relationColumnValues(from, to)).build
  def deleteDirectedRelationStatement(keyspace: String, tableName: String, from: UUID, to: UUID): SimpleStatement = deleteStatement(keyspace, tableName, relationColumnValues(from, to)).build

  def updateBidirectionalRelation(statement: (String, String,UUID,UUID) => SimpleStatement, model: OutputModel, fromEntity: String, fromRelationName: String): (UUID,UUID)=>List[SimpleStatement] = {
    val fromRelation =  model.input.entities(fromEntity).relations(fromRelationName)
    val toEntity = fromRelation.targetEntityName
    val toRelationName = fromRelation.inverseName
    val fromTable = model.relationTables((fromEntity, fromRelationName))
    val toTable = model.relationTables((toEntity, toRelationName))
    (fromId: UUID, toId: UUID) => List(statement(fromTable.keyspace, fromTable.name, fromId, toId), statement(toTable.keyspace, toTable.name, toId, fromId))
  }
  def createBidirectionalRelation(model: OutputModel, fromEntity: String, fromRelationName: String): (UUID,UUID)=>List[SimpleStatement] = {
    updateBidirectionalRelation(createDirectedRelationStatement, model, fromEntity, fromRelationName)
  }
  def deleteBidirectionalRelation(model: OutputModel, fromEntity: String, fromRelationName: String): (UUID,UUID)=>List[SimpleStatement] = {
    updateBidirectionalRelation(deleteDirectedRelationStatement, model, fromEntity, fromRelationName)
  }

}
