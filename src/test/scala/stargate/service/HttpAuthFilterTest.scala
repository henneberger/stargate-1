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

import org.junit.Test
import org.junit.BeforeClass
import org.junit.AfterClass
import stargate.CassandraTest
import scala.util.Random
import stargate.service.testsupport._
import org.junit.Assert._
import org.hamcrest.CoreMatchers._
import org.junit.Before
import com.typesafe.scalalogging.LazyLogging

object HttpAuthFilterTest extends CassandraTest {
  
  var sc: ServletContext = _
  var namespace: String = _

  @BeforeClass
  def startup():Unit = {
    val rand = new Random()
    this.init()
    val namespace = this.registerKeyspace(s"sgTest${rand.nextInt(10000)}")
    val systemKeyspace = this.newKeyspace()
    val authEnabled = true
    sc = startServlet(9091, true, logger, systemKeyspace, rand, namespace, this.clientConfig)
  }

  @AfterClass
  def shutdown(): Unit = {
    sc.shutdown()
    this.cleanup()
  }
}

class HttpAuthFilterTest extends LazyLogging{
  val baseUrl = "http://localhost:9091/api-docs"
  var sc: ServletContext = _
  val user = "admin"
  val password = "sgAdmin1234"

  @Before
  def setup(): Unit = {
    logger.info("wiring started context to setup")
    if (HttpAuthFilterTest.sc == null){
      throw new RuntimeException("never setup the servlet correctly")
    }
    sc = HttpAuthFilterTest.sc
  }

  @Test
  def testSwaggerDenied(){
    val swaggerResponse = httpGet(s"$baseUrl/swagger.json", "application/json", "")
    assertThat(swaggerResponse.statusCode, equalTo(401))
    val swaggerHtmlResponse = httpGet(s"$baseUrl/${sc.namespace}/swagger", "application/json", "")
    assertThat(swaggerHtmlResponse.statusCode, equalTo(401))
    val swaggerEntityResponse = httpGet(s"$baseUrl/${sc.namespace}/swagger.json", "application/json", "")
    assertThat(swaggerEntityResponse.statusCode, equalTo(401))

  }

  @Test
  def testSwaggerDeniedWithInvalidPass(){
    val swaggerResponse = httpGet(s"$baseUrl/swagger.json", "application/json", "", user, "badpass")
    assertThat(swaggerResponse.statusCode, equalTo(401))
    val swaggerEntityResponse = httpGet(s"$baseUrl/${sc.namespace}/swagger.json", "application/json", "", user, "badpass")
    assertThat(swaggerEntityResponse.statusCode, equalTo(401))
    val swaggerHtmlResponse = httpGet(s"$baseUrl/${sc.namespace}/swagger", "application/json", "", user, "badpass")
    assertThat(swaggerHtmlResponse.statusCode, equalTo(401))
  }

  @Test
  def testSwaggerAllowsWithValidUserPass(){
    val swaggerResponse = httpGet(s"$baseUrl/swagger.json", "application/json", "", user, password)
    assertThat(swaggerResponse.statusCode, equalTo(200))
    val swaggerHtmlResponse = httpGet(s"$baseUrl/${sc.namespace}/swagger", "application/json", "", user, password)
    assertThat(swaggerHtmlResponse.statusCode, equalTo(200))
    val swaggerEntityResponse = httpGet(s"$baseUrl/${sc.namespace}/swagger.json", "application/json", "", user, password)
    assertThat(swaggerEntityResponse.statusCode, equalTo(200))
  }
}
