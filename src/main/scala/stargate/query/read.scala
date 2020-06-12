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

import java.lang
import java.util.UUID

import stargate.model.{OutputModel, ScalarComparison, ScalarCondition}
import stargate.util.AsyncList
import stargate.{cassandra, schema}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause
import com.datastax.oss.driver.api.querybuilder.select.{Select, SelectFrom}
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.datastax.oss.driver.internal.core.util.Strings
import stargate.cassandra.{CassandraColumn, CassandraTable}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

// functions used to implement stargate queries
object read {

  def appendWhere[T <: OngoingWhereClause[T]](select: T, condition: ScalarCondition[Object]): T = {
    val where = select.whereColumn(Strings.doubleQuote(condition.field))
    def literal: Term = QueryBuilder.literal(condition.argument)
    def listLiteral: java.lang.Iterable[Term] = condition.argument.asInstanceOf[List[Object]].map(QueryBuilder.literal(_):Term).asJava
    condition.comparison match {
      case ScalarComparison.LT => where.isLessThan(literal)
      case ScalarComparison.LTE => where.isLessThanOrEqualTo(literal)
      case ScalarComparison.EQ => where.isEqualTo(literal)
      case ScalarComparison.GTE => where.isGreaterThanOrEqualTo(literal)
      case ScalarComparison.GT => where.isGreaterThan(literal)
      case ScalarComparison.IN => where.in(listLiteral)
    }
  }
  def appendWhere[T <: OngoingWhereClause[T]](select: T, conditions: List[ScalarCondition[Object]]): T = {
    conditions.foldLeft(select)((select, cond) => appendWhere(select, cond))
  }
  def tableConditionsForEntity(table: CassandraTable, entity: Map[String,Object]): List[Option[ScalarCondition[Object]]] = {
    table.columns.key.combined.map(col => {
      entity.get(col.name).map(value => ScalarCondition(col.name, ScalarComparison.EQ, value))
    })
  }
  def physicalConditions(columns: Map[String,CassandraColumn], conditions: List[ScalarCondition[Object]]): List[ScalarCondition[Object]] = {
    conditions.map(cond => {
      val column = columns(cond.field)
      val argument = if(cond.comparison != ScalarComparison.IN) {
        column.physicalValue(cond.argument)
      } else {
        cond.argument.asInstanceOf[List[Object]].map(column.physicalValue)
      }
      cond.replaceArgument(argument)
    })
  }


  def selectStatement(keyspace: String, tableName: String, columns: SelectFrom=>Select, conditions: List[ScalarCondition[Object]]): Select = {
    val select = QueryBuilder.selectFrom(Strings.doubleQuote(keyspace), Strings.doubleQuote(tableName))
    val selectColumns = columns(select)
    appendWhere(selectColumns.where(), conditions)
  }
  def selectStatement(keyspace: String, tableName: String, conditions: List[ScalarCondition[Object]]): Select = selectStatement(keyspace, tableName, _.all(), conditions)
  def selectStatement(keyspace: String, tableName: String, columns: List[String], conditions: List[ScalarCondition[Object]]): Select = {
    selectStatement(keyspace, tableName, selectFrom => columns.foldLeft(selectFrom.columns(List.empty[String].asJava))((select, col) => select.column(Strings.doubleQuote(col))), conditions)
  }
  def selectStatement(keyspace: String, tableName: String, maybeColumns: Option[List[String]], conditions: List[ScalarCondition[Object]]): Select = {
    maybeColumns.map(selectStatement(keyspace, tableName, _, conditions)).getOrElse(selectStatement(keyspace, tableName, conditions))
  }
  def selectStatement(table: CassandraTable, conditions: List[ScalarCondition[Object]]): Select = {
    selectStatement(table.keyspace, table.name, physicalConditions(table.columns.combinedMap, conditions))
  }
  def selectStatement(table: CassandraTable, values: Map[String,Object]): Select = {
    selectStatement(table, values.toList.map(v => ScalarCondition[Object](v._1, ScalarComparison.EQ, v._2)))
  }

  def relatedSelect(keyspace: String, relationTable: String, fromIds: List[UUID], session: CqlSession,  executor: ExecutionContext): AsyncList[UUID] = {
    val conditions = List(ScalarCondition[Object](schema.RELATION_FROM_COLUMN_NAME, ScalarComparison.IN, fromIds))
    val rows = cassandra.queryAsync(session, selectStatement(keyspace, relationTable, conditions).build, executor)
    rows.map(_.getUuid(schema.RELATION_TO_COLUMN_NAME), executor)
  }

  def entityIdToObject(context: Context, entityName: String, maybeColumns: Option[List[String]], id: UUID): Future[Option[Map[String,Object]]] = {
    val baseTable = context.model.baseTables(entityName)
    val select = selectStatement(baseTable.keyspace, baseTable.name, maybeColumns, List(ScalarCondition[Object](schema.ENTITY_ID_COLUMN_NAME, ScalarComparison.EQ, id)))
    cassandra.queryAsync(context.session, select.build, context.executor).maybeHead(context.executor).map(_.map(cassandra.rowToMap))(context.executor)
  }
  def entityIdToObject(context: Context, entityName: String, id: UUID): Future[Option[Map[String, Object]]] = {
    entityIdToObject(context, entityName, None, id)
  }
}
