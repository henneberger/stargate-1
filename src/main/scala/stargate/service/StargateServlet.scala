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
package stargate.service

import java.util.UUID
import java.util.concurrent._

import com.datastax.oss.driver.api.core.CqlSession
import com.swrve.ratelimitedlogger.RateLimitedLog
import com.typesafe.scalalogging.LazyLogging
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import stargate.cassandra.CassandraTable
import stargate.model.{OutputModel, ScalarComparison, generator, queries}
import stargate.query.pagination.{StreamEntry, Streams, TruncateResult}
import stargate.service.config.StargateConfig
import stargate.service.metrics.RequestCollector
import stargate.{cassandra, keywords, query, util}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try

class StargateServlet(
                       val sgConfig: StargateConfig,
                       val cqlSession: CqlSession,
                       val apps: Namespaces,
                       val datamodelRepoTable: CassandraTable,
                       val executor: ExecutionContextExecutor
) extends HttpServlet
    with RequestCollector
    with LazyLogging {
  val continuationCache = new ConcurrentHashMap[UUID, (StreamEntry, ScheduledFuture[Unit])]()
  val continuationCleaner: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  val maxSchemaSize: Long = sgConfig.maxSchemaSizeKB * 1024
  val maxMutationSize: Long = sgConfig.maxMutationSizeKB * 1024
  val maxRequestSize: Long = sgConfig.maxRequestSizeKB * 1024
  val rateLimitedLog: RateLimitedLog = RateLimitedLog
    .withRateLimit(logger.underlying)
    .maxRate(5)
    .every(java.time.Duration.ofSeconds(10))
    .build()

  def route(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    try {
      val contentLength = req.getContentLengthLong
      http.validateRequestSize(contentLength, maxRequestSize)
      val op = req.getMethod
      val path = http.sanitizePath(req.getPathInfo())
      logger.debug(s"Stargate PATH from servlet is $path")
      logger.trace(
        s"http request: { path: '$path', method: '$op', content-length: $contentLength, content-type: '${req.getContentType}' }"
      )
      path match {
        case s"/${namespace}/schema" =>
          logger.trace("matched /:namespace/schema")
          http.validateSchemaSize(contentLength, maxSchemaSize)
          val input = new String(req.getInputStream.readAllBytes)
          op match {
            case "DELETE" => deleteSchema(namespace, resp)
            case "POST" =>
              http.validateHoconHeader(req)
              postSchema(namespace, input, resp)
            case _ =>
              resp.setStatus(HttpServletResponse.SC_NOT_FOUND)
              rateLimitedLog.warn(s"invalid method '$op' for schema operations")
          }
        case s"/validate" => //TODO this is probably the wrong url.need to update swagger when we fix this
          logger.trace("matched /validate")
          http.validateHoconHeader(req)
          http.validateSchemaSize(contentLength, maxSchemaSize)
          val input = new String(req.getInputStream.readAllBytes)
          val model = stargate.model.parser.parseModel(input)
          resp.getWriter.write(util.toJson(model))
        case s"/${namespace}/apigen/${entityName}/${op}" =>
          logger.trace("matched /:namespace/apigen/:entityName")
          generateQuery(namespace, entityName, op, resp)
        case s"/${namespace}/query/stored/${queryName}" =>
          logger.trace("matched /:namespace/query/stored/:queryName")
          //http.validateJsonContentHeader(req)
          val isSwagger = contentLength < 1L
          var payload: String = ""
          if (isSwagger) {
            payload = req.getParameter("payload")
          } else {
            payload = new String(req.getInputStream.readAllBytes)
          }
          logger.trace(s"stored query payload is $payload")
          runPredefinedQuery(namespace, queryName, payload, resp)
        case s"/${namespace}/query/continue/${id}" => {
          logger.trace("matched /:namespace/query/continue/:id")
          continueQuery(namespace, UUID.fromString(id), resp)
        }
        case s"/${namespace}/query/entity/${entity}/${id}" => {
          logger.trace("matched /:namespace/query/:entity/:id")
          require(op != "POST", "cannot create entity with id specified in path")
          http.validateMutation(op, contentLength, maxMutationSize)
          val payload = new String(req.getInputStream.readAllBytes)
          logger.trace(s"entity $op payload is $payload")
          val condition = Map((stargate.keywords.mutation.MATCH, List(stargate.schema.ENTITY_ID_COLUMN_NAME, ScalarComparison.EQ.toString, UUID.fromString(id))))
          val payloadObj = if(payload.nonEmpty) {
            http.validateJsonContentHeader(req)
            util.fromJson(payload).asInstanceOf[Map[String,Object]] ++ condition
          } else {
            condition
          }
          logger.trace(s"entity converts to $payloadObj")
          runQuery(namespace, entity, op, payloadObj , resp)
        }
        case s"/${namespace}/query/entity/${entity}" => {
          logger.trace("matched /:namespace/query/:entity/")
          http.validateMutation(op, contentLength, maxMutationSize)
          val isSwagger = contentLength < 1L
          var payload: String = ""
          if (isSwagger) {
            logger.trace(s"parameter names are ${req.getQueryString}")
            payload = req.getParameter("payload")
          } else {
            http.validateJsonContentHeader(req)
            payload = new String(req.getInputStream.readAllBytes)
          }
          logger.trace(s"entity $op payload is $payload")
          val payloadObj = util.fromJson(payload)
          logger.trace(s"entity converts to $payloadObj")
          runQuery(namespace, entity, op, payloadObj, resp)
        }
        case _ =>
          resp.setStatus(HttpServletResponse.SC_NOT_FOUND)
          val msg = s"invalid path: $path, see swagger api-docs for available endpoints: /${StargateApiVersion}/api-docs/:namespace/swagger"
          rateLimitedLog.warn(msg)
          resp.getWriter.write(util.toJson(msg))
      }
    } catch {
      case e: Exception =>
        rateLimitedLog.error(s"exception: $e")
        resp.setStatus(HttpServletResponse.SC_BAD_GATEWAY)
        resp.getWriter.write(util.toJson(e.getMessage))
    }
  }

  def lookupModel(appName: String): OutputModel = {
    val model = this.apps.get(appName)
    require(model != null, s"invalid database name: $appName")
    model
  }

  def postSchema(appName: String, input: String, response: HttpServletResponse): Unit = {
    val model = stargate.schema.outputModel(stargate.model.parser.parseModel(input), appName)
    val previousDatamodel =
      util.await(datamodelRepository.fetchLatestDatamodel(appName, datamodelRepoTable, cqlSession, executor)).get
    if (!previousDatamodel.contains(input)) {
      logger.info(s"""creating keyspace "$appName" for new datamodel""")
      datamodelRepository.updateDatamodel(appName, input, datamodelRepoTable, cqlSession, executor)
      cassandra.recreateKeyspace(cqlSession, appName, sgConfig.cassandra.cassandraReplication)
      Await.result(model.createTables(cqlSession, executor), Duration.Inf)
    } else {
      logger.info(s"""reusing existing keyspace "$appName" with latest datamodel""")
    }
    val namespace = apps.put(appName, model)
    response.setContentType("application/json")
    response.getWriter.write(util.toJson(namespace))
  }

  def deleteSchema(appName: String, resp: HttpServletResponse): Unit = {
    logger.info(s"""deleting datamodels and keyspace for app "$appName" """)
    val removed = this.apps.remove(appName)
    util.await(datamodelRepository.deleteDatamodel(appName, datamodelRepoTable, cqlSession, executor)).get
    cassandra.wipeKeyspace(cqlSession, appName)
    if (removed == null) {
      resp.setStatus(404)
    }
  }

  def generateQuery(appName: String, entity: String, op: String, resp: HttpServletResponse): Unit = {
    val model = lookupModel(appName)
    require(model.input.entities.contains(entity), s"""database "$appName" does not have an entity named "$entity" """)
    val validOps = Set("create", "get", "update", "delete")
    require(requirement = validOps.contains(op), message = s"operation $op must be one of the following: $validOps")
    val requestF = op match {
      case "create" => generator.specificCreateRequest(model, entity, cqlSession, executor)
      case "get"    => generator.specificGetRequest(model, entity, 3, cqlSession, executor)
      case "update" => generator.specificUpdateRequest(model, entity, cqlSession, executor)
      case "delete" => generator.specificDeleteRequest(model, entity, cqlSession, executor)
    }
    val request = util.await(requestF).get
    resp.setContentType("application/json")
    resp.getWriter.write(util.toJson(request))
  }

  def runPredefinedQuery(appName: String, queryName: String, input: String, resp: HttpServletResponse): Unit = {
    val model = lookupModel(appName)
    val payloadMap = util.fromJson(input).asInstanceOf[Map[String, Object]]
    val query = Try(model.input.queries(queryName))
    require(query.isSuccess, s"""no such query "$queryName" for database "$appName" """)
    val runtimePayload = queries.predefined.transform(query.get, payloadMap)
    val result = stargate.query.getAndTruncate(
      stargate.query.Context(model, cqlSession, executor),
      query.get.entityName,
      runtimePayload,
      sgConfig.defaultLimit,
      sgConfig.defaultTTL
    )
    val entities = cacheStreams(result)
    resp.setContentType("application/json")
    resp.getWriter.write(util.toJson(Await.result(entities, Duration.Inf)))
  }

  def cacheStreams(truncatedFuture: TruncateResult): Future[Map[String, Object]] = {
    truncatedFuture.map(truncated_streams => {
      val (truncated, metadata, streams) = truncated_streams
      streams.foreach(stream => {
        // do not allow cleanup to run until stream is actually added to cache
        val lock = new Semaphore(0)
        val cleanup: ScheduledFuture[Unit] =
          continuationCleaner.schedule(
            () => {
              logger.trace("cleanup", continuationCache.keys, "-", stream._1)
              lock.acquire()
              continuationCache.remove(stream._1)
              ()
            },
            stream._2.ttl,
            TimeUnit.SECONDS
          )
        continuationCache.put(stream._1, (stream._2, cleanup))
        lock.release()
      })
      Map((keywords.response.DATA, truncated), (keywords.response.continue.ROOT, metadata))
    })(executor)
  }

  def continueQuery(appName: String, continueId: UUID, resp: HttpServletResponse): Unit = {
    val model = lookupModel(appName)
    val continue_cleanup = continuationCache.remove(continueId)
    require(continue_cleanup != null, s"""no continuable query found for id $continueId in database "$appName" """)
    val (entry, cleanup) = continue_cleanup
    cleanup.cancel(false)

    val truncateFuture = stargate.query.pagination.truncate(
      model.input,
      entry.entityName,
      entry.getRequest,
      entry.entities,
      continueId,
      sgConfig.defaultLimit,
      sgConfig.defaultTTL,
      executor
    )
    val entities = Await.result(cacheStreams(truncateFuture), Duration.Inf)
    resp.setContentType("application/json")
    resp.getWriter.write(util.toJson(entities))
  }

  def runQuery(appName: String, entity: String, op: String, input: String, resp: HttpServletResponse): Unit = {
    val payload = util.fromJson(input)
    runQuery(appName, entity, op, payload, resp)
  }

  def runQuery(appName: String, entity: String, op: String, payload: Object, resp: HttpServletResponse): Unit = {
    val model = lookupModel(appName)
    val crud = stargate.model.unbatchedCRUD(model, cqlSession, executor)
    require(model.input.entities.contains(entity), s"""database "$appName" does not have an entity named "$entity" """)
    val payloadMap = Try(payload.asInstanceOf[Map[String, Object]])
    logger.trace(s"query payload: $payload")

    def wrapResponse(o: Object) = Map((keywords.response.DATA, o))
    val result: Future[Object] = op match {
      case "GET" =>
        val result = query.untyped.getAndTruncate(model, entity, payloadMap.get, sgConfig.defaultLimit, sgConfig.defaultTTL, cqlSession, executor)
        cacheStreams(result)
      case "POST"   => crud.create(entity, payload).map(wrapResponse)(executor)
      case "PUT"    => crud.update(entity, payloadMap.get).map(wrapResponse)(executor)
      case "DELETE" => crud.delete(entity, payloadMap.get).map(wrapResponse)(executor)
      case _        => Future.failed(new RuntimeException(s"unsupported op: $op"))
    }
    logger.trace(op, Await.result(result, Duration.Inf))
    resp.setContentType("application/json")
    resp.getWriter.write(util.toJson(Await.result(result, Duration.Inf)))
  }

  override def doPut(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    route(req, resp)
  }

  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    route(req, resp)
  }

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    route(req, resp)
  }

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    route(req, resp)
  }

}
