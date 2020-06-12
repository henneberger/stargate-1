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

import com.datastax.oss.driver.api.core.CqlSession
import stargate.model.queries.parser
import stargate.model.{Entities, OutputModel}
import stargate.query
import stargate.query.pagination.TruncateResult
import stargate.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}

object untyped {

  def validateThenQuery[I,V,O](validate: (Entities, String, I) => V, query: (Context, String, V) => O): (OutputModel, String, I, CqlSession, ExecutionContext) => O = {
    (model: OutputModel, entityName: String, input: I, session: CqlSession, executor: ExecutionContext) => {
      query(Context(model, session, executor), entityName, validate(model.input.entities, entityName, input))
    }
  }

  val get: (OutputModel, String, Map[String, Object], CqlSession, ExecutionContext) => AsyncList[Map[String, Object]] = validateThenQuery(parser.parseGet, query.get)
  def getAndTruncate(model: OutputModel, entityName: String, payload: Map[String,Object], defaultLimit: Int, defaultTTL: Int, session: CqlSession, executor: ExecutionContext): TruncateResult = {
    query.getAndTruncate(Context(model, session, executor), entityName, parser.parseGet(model.input.entities, entityName, payload), defaultLimit, defaultTTL)
  }
  def getAndTruncate(model: OutputModel, entityName: String, payload: Map[String,Object], defaultLimit: Int, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    getAndTruncate(model, entityName, payload, defaultLimit, 0, session, executor).map(_._1)(executor)
  }

  val createUnbatched: (OutputModel, String, Object, CqlSession, ExecutionContext) => Future[List[Map[String, Object]]] = validateThenQuery(parser.parseCreate, query.createUnbatched)
  val createBatched: (OutputModel, String, Object, CqlSession, ExecutionContext) => Future[List[Map[String, Object]]] = validateThenQuery(parser.parseCreate, query.createBatched)

  val updateUnbatched: (OutputModel, String, Map[String, Object], CqlSession, ExecutionContext) => Future[List[Map[String, Object]]] = validateThenQuery(parser.parseUpdate, query.updateUnbatched)
  val updateBatched: (OutputModel, String, Map[String, Object], CqlSession, ExecutionContext) => Future[List[Map[String, Object]]] = validateThenQuery(parser.parseUpdate, query.updateBatched)

  val deleteUnbatched: (OutputModel, String, Map[String, Object], CqlSession, ExecutionContext) => Future[List[Map[String, Object]]] = validateThenQuery(parser.parseDelete, query.deleteBatched)
  val deleteBatched: (OutputModel, String, Map[String, Object], CqlSession, ExecutionContext) => Future[List[Map[String, Object]]] = validateThenQuery(parser.parseDelete, query.deleteUnbatched)

}
