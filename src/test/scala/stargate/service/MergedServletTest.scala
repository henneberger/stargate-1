package stargate.service

import org.junit.{AfterClass, BeforeClass}
import stargate.CassandraTest
import stargate.service.config.StargateConfig
import stargate.service.testsupport._

import scala.io.Source
import scala.util.Random

object MergedServletTest extends CassandraTest {
  var sc: ServletContext = _
  val rand: Random = new Random()
  @BeforeClass
  def startup() = {
    this.init()
    val namespace = this.registerKeyspace(s"sgTest${rand.nextInt(10000)}")
    val systemKeyspace = this.newKeyspace()
    val clientConfig = this.clientConfig
    val authEnabled = false
    sc = startServlet(9090, false, logger, systemKeyspace, rand, namespace, this.clientConfig)
  }

  @AfterClass
  def shutdown() = {
    sc.shutdown() 
    this.cleanup()
    logger.info("test complete executing cleanup")
  }
}

class MergedServletTest extends QueryServletTest with StargateServletTest with SwaggerServletTest {

  override def registerKeyspace(keyspace: String): String = MergedServletTest.registerKeyspace(keyspace) }
