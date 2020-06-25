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

package stargate
import java.nio.file.Paths
import java.util.concurrent.ConcurrentSkipListSet

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.scalalogging.LazyLogging
import stargate.service.config.CassandraClientConfig

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Random, Try}

trait CassandraTestSession {
  def session: CqlSession
  def newKeyspace(): String
}
trait KeyspaceRegistry {
  def registerKeyspace(keyspace: String): String
}

trait CassandraTest extends LazyLogging {

  val image = "cassandra:3.11.6"
  private val persistentDseContainer = "stargate-tests-cassandra"
  private val dataCenter: String = "datacenter1"

  private var cqlPort: Int = _
  private var dockerStarted: Boolean = true
  var cqlSession: CqlSession = _
  private val keyspaces: ConcurrentSkipListSet[String] = new java.util.concurrent.ConcurrentSkipListSet[String]()


  def ensureCassandraRunning(): Unit = {
    def getPort(): Try[Int] = util.process.exec(List("docker", "port", persistentDseContainer, "9042")).map(out => Integer.parseInt(out.trim.split(":")(1)))
    val existing: Try[Any] = getPort().map(port => { dockerStarted = false; logger.info("using existing cassandra docker at port: {}", port)})
    val restarted: Try[Any] = existing.orElse(util.process.exec(List("docker", "restart", persistentDseContainer)).map(_ => logger.info("restarting old container: {}", persistentDseContainer)))
    val created: Try[Any] = restarted.orElse({
      val testConfig = Paths.get("src", "test", "resources", "cassandra.yaml").toAbsolutePath
      val command = List("docker", "run", "-d", "-P", "-v", s"${testConfig.toString}:/etc/cassandra/cassandra.yaml", "--name", persistentDseContainer, image)
      logger.warn(s"""starting cassandra docker container for test: ${persistentDseContainer}
                     |    you can start the container yourself if you dont want to wait for the container to start:
                     |    ${command.mkString(" ")}""".stripMargin)
      util.process.exec(command)
    })
    this.cqlPort = getPort().get
  }

  def clientConfig: CassandraClientConfig = {
    CassandraClientConfig(List(("localhost", cqlPort)), dataCenter, 1, "cassandra", "cassandra", "PlainTextAuthProvider")
  }

  def newKeyspace(): String = {
    val keyspace = ("keyspace" + Random.alphanumeric.take(10).mkString).toLowerCase
    logger.info("creating keyspace: {}", keyspace)
    cassandra.createKeyspace(cqlSession, keyspace, 1)
    registerKeyspace(keyspace)
  }
  def registerKeyspace(keyspace: String): String = {
    this.keyspaces.add(keyspace)
    keyspace
  }
  private def cleanupKeyspace(keyspace: String): Unit = {
    val wiped = Try(cassandra.wipeKeyspace(cqlSession, keyspace))
    logger.info(s"cleaning up keyspace ${keyspace}: ${wiped.toString}")
  }


  // make this @BeforeClass for any tests that depend on cassandra
  def init(): Unit = {
    ensureCassandraRunning()
    logger.info("creating CqlSession with config: {}", this.clientConfig)
    cqlSession = util.retry(cassandra.session(this.clientConfig), Duration("60s"), Duration("5s")).get
  }

  // make this @AfterClass for any tests that depend on cassandra
  def cleanup(): Unit = {
    keyspaces.asScala.foreach(cleanupKeyspace)
    cqlSession.close()
    if(dockerStarted) {
      util.process.exec(List("docker", "kill", persistentDseContainer)).get
    }
  }
}
