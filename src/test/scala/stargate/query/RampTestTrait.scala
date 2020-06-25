package stargate.query

import java.util.UUID
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import java.util.function.Supplier

import com.typesafe.config.ConfigFactory
import org.junit.Assert._
import org.junit.Test
import stargate.model.{CRUD, parser}
import stargate.{CassandraTestSession, util}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._

trait RampTestTrait extends CassandraTestSession {

  val entityName: String = "A"
  val threads = 5
  val maxValue = 100

  def increment(crud: CRUD, id: UUID, field: String, max: Int, executor: ExecutionContext): Future[Unit] = {
    val req = Map(("-match", List("entityId", "=", id)))
    List.range(0, max).foldLeft(Future.unit)((f, i) => {
      f.flatMap(_ => crud.update(entityName, req ++ Map((field, java.lang.Integer.valueOf(i)))).map(_ => ())(executor))(executor)
    })
  }

  def runTest(crud: CRUD, max: Int, executor: ExecutorService): Unit = {
    val createReq = List.range(0, threads).map(i => ("a" + i, java.lang.Integer.valueOf(0))).toMap
    val createResp = util.await(crud.create(entityName, createReq)).get
    val id = createResp(0)("entityId").asInstanceOf[UUID]
    val ec = ExecutionContext.fromExecutor(executor)
    val threadFs: List[Future[Future[Unit]]] = List.range(0,threads).map(i => {
      val run: Supplier[Future[Unit]] = () => increment(crud, id, "a" + i, max, ec)
      CompletableFuture.supplyAsync(run, executor).asScala
    })
    util.await(util.sequence(threadFs.map(_.flatten), ec)).get
    val getReq = Map(("-match", List("entityId", "=", id)))
    val getResp = util.await(crud.getAndTruncate(entityName, getReq, 1000)).get(0)
    List.range(0, threads).foreach(i =>
      assertEquals(getResp("a" + i), max - 1)
    )
  }

  @Test
  def raceConditionTest = {
    val executor = Executors.newCachedThreadPool()
    val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    val inputModel = parser.parseModel(ConfigFactory.parseResources("race-schema.conf"))
    val keyspace = newKeyspace()
    val model = stargate.schema.rampOutputModel(inputModel, keyspace)
    util.await(model.createRampTables(session, ec)).get
    val crud = stargate.model.rampCRUD(model, this.session, ec)
    runTest(crud, maxValue, executor)
  }


}
