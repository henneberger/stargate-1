package stargate.query

import com.datastax.oss.driver.api.core.CqlSession
import stargate.model.queries.parser
import stargate.model.{Entities, InputModel, OutputModel}
import stargate.query
import stargate.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}

object untyped {

  def validateThenQuery[I,V,O](validate: (Entities, String, I) => V, query: (OutputModel, String, V, CqlSession, ExecutionContext) => O): (OutputModel, String, I, CqlSession, ExecutionContext) => O = {
    (model: OutputModel, entityName: String, input: I, session: CqlSession, executor: ExecutionContext) => {
      query(model, entityName, validate(model.input.entities, entityName, input), session, executor)
    }
  }

  val get: (OutputModel, String, Map[String, Object], CqlSession, ExecutionContext) => AsyncList[Map[String, Object]] = validateThenQuery(parser.parseGet, query.get)
  def getAndTruncate(model: OutputModel, entityName: String, payload: Map[String,Object], defaultLimit: Int, defaultTTL: Int, session: CqlSession, executor: ExecutionContext): Future[(List[Map[String,Object]], pagination.Streams)] = {
    query.getAndTruncate(model, entityName, parser.parseGet(model.input.entities, entityName, payload), defaultLimit, defaultTTL, session, executor)
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
