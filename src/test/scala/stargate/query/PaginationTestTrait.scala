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
import com.typesafe.config.ConfigFactory
import org.junit.Test
import stargate.model.{OutputModel, parser}
import stargate.{CassandraTestSession, util}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

trait PaginationTestTrait extends CassandraTestSession {

  def generate(model: OutputModel, branching: Int, session: CqlSession, executor: ExecutionContext): Future[Unit] = {
    val c = List.range(0, branching).map(_ => Map.empty[String,Object])
    val b = List.range(0, branching).map(_ => Map(("c", c)))
    val a = List.range(0, branching).map(_ => Map(("b", b), ("c", c)))
    model.mutation.create("A", a, session, executor).map(_ => ())(executor)
  }

  def query(model: OutputModel, limit: Int, branching: Int, session: CqlSession, executor: ExecutionContext): Unit = {
    val req = Map[String,Object](
      (stargate.keywords.mutation.MATCH, "all"),
      (stargate.keywords.pagination.LIMIT, Integer.valueOf(limit)),
      (stargate.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
      ("b", Map(
        (stargate.keywords.pagination.LIMIT, Integer.valueOf(limit)),
        (stargate.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
        ("c", Map(
          (stargate.keywords.pagination.LIMIT, Integer.valueOf(limit)),
          (stargate.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
        )),
      ))
    )
    val (entities, continue, streams) = Await.result(stargate.query.untyped.getAndTruncate(model, "A", req, limit, 0, session, executor), Duration.Inf)
    assert(entities.length <= limit)
    streams.values.foreach((ttl_stream) => Await.result(ttl_stream.entities.length(executor), Duration.Inf) == branching - limit)
  }

  def paginationTest(model: OutputModel, branching: Int, limit: Int, session: CqlSession, executor: ExecutionContext): Unit = {
    Await.result(generate(model, branching, session, executor), Duration.Inf)
    query(model, limit, branching, session, executor)
  }

  @Test
  def paginationTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("pagination-schema.conf"))
    val keyspace = newKeyspace()
    val model = stargate.schema.outputModel(inputModel, keyspace)
    val executor = ExecutionContext.global
    util.await(model.createTables(session, executor)).get
    paginationTest(model, 5, 3, session, executor)
  }
}
