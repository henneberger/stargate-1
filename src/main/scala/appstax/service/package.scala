package appstax

import java.io.File
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

import appstax.model.OutputModel
import appstax.util.AsyncList
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{Config, ConfigFactory}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.servlet.{ServletHandler, ServletHolder}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try


class AppstaxServlet(val config: Config) extends HttpServlet {

  val apps = new ConcurrentHashMap[String, (CqlSession, OutputModel)]()
  val om: ObjectMapper = new ObjectMapper()
  val executor: ExecutionContext = ExecutionContext.global

  def newSession(keyspace: String): CqlSession = {
    val contacts = config.getConfig("cassandra").getConfigList("contactPoints").asScala
    val builder = contacts.foldLeft(CqlSession.builder)((builder, config) => builder.addContactPoint(InetSocketAddress.createUnresolved(config.getString("host"), config.getInt("port"))))
    val withoutKeyspace = builder.withLocalDatacenter(config.getString("cassandra.dataCenter"))
    val sessionWithoutKeyspace = withoutKeyspace.build
    sessionWithoutKeyspace.execute(SchemaBuilder.dropKeyspace(keyspace).ifExists.build)
    sessionWithoutKeyspace.execute(SchemaBuilder.createKeyspace(keyspace).withSimpleStrategy(config.getInt("cassandra.replication")).build)
    sessionWithoutKeyspace.close
    withoutKeyspace.withKeyspace(keyspace).build
  }

  def truncateAsyncLists(results: Object, pageSize: Int): Object = {
    results match {
      case x: AsyncList[Object] => Await.result(x.take(pageSize, executor), Duration.Inf)
      case x: List[Object] => x.map(truncateAsyncLists(_, pageSize))
      case x: Map[String,Object] => x.iterator.map(kv => (kv._1, truncateAsyncLists(kv._2, pageSize))).toMap
      case x => x
    }
  }
  def truncateAsyncLists[T](results: AsyncList[T], pageSize: Int): Future[Object] = {
    results.take(pageSize, executor).map(truncateAsyncLists(_, pageSize))(executor)
  }

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val path = req.getServletPath.split("/").toList.filter(_.nonEmpty)
    val appName = path(0)
    val input = new String(req.getInputStream.readAllBytes())
    if(path.length == 1) {
      Try(apps.get(appName)._1.close)
      val model = appstax.schema.outputModel(appstax.model.parser.parseModel(input))
      val session = newSession(appName)
      implicit val ec: ExecutionContext = executor
      Await.ready(Future.sequence(model.tables.map(cassandra.create(session, _))), Duration.Inf)
      apps.put(appName, (session, model))
      resp.getWriter.write(model.toString)

    } else if(path.length == 3) {
      val (session, model) = apps.get(appName)
      val entity = path(1)
      val op = path(2)
      val payload = util.javaToScala(om.readValue(input, classOf[Object]))
      val payloadMap = Try(payload.asInstanceOf[Map[String,Object]])
      println(payload)
      val result: Future[Object] = op match {
        case "get" => truncateAsyncLists(model.getWrapper(entity)(session, payloadMap.get, executor), 20)
        case "create" => model.createWrapper(entity)(session, payload, executor)
        case "update" => model.updateWrapper(entity)(session, payloadMap.get, executor)
        case "delete" => model.deleteWrapper(entity)(session, payloadMap.get, executor)
        case _ => Future.failed(new RuntimeException(s"unsupported op: ${op}"))
      }
      println(op, Await.result(result, Duration.Inf))
      resp.getWriter.write(om.writeValueAsString(util.scalaToJava(Await.result(result, Duration.Inf))))
    }
  }
}


object Main {

  def main(args: Array[String]) = {
    val config = if (args.length > 0) ConfigFactory.parseFile(new File(args(0))) else ConfigFactory.defaultApplication()

    val server = new org.eclipse.jetty.server.Server(config.getInt("http.port"))
    val handler = new ServletHandler()
    val servlet = new AppstaxServlet(config)

    handler.addServletWithMapping(new ServletHolder(servlet), "/")
    server.setHandler(handler)
    server.start
    server.join

  }
}

package object service extends scala.App {

  Main.main(args)

}