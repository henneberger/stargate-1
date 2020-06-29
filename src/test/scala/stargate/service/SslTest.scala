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

import org.hamcrest.CoreMatchers._
import org.junit.{AfterClass, BeforeClass}
import org.junit.Test
import org.junit.Assert._
import stargate.service.testsupport._
import stargate.service.config.StargateConfig
import stargate.CassandraTest
import scala.util.Random
import org.junit.Before

object SslTest extends CassandraTest {
  var sc: ServletContext = _
  val rand: Random = new Random()

  @BeforeClass
  def startup() = {
    this.init()
    val namespace = this.registerKeyspace(s"sgTest${rand.nextInt(10000)}")
    val systemKeyspace = this.newKeyspace()
    val clientConfig = this.clientConfig
    val authEnabled = false
    sc = startServlet(9092, false, logger, systemKeyspace, rand, namespace, this.clientConfig, true)
  }

  @AfterClass
  def shutdown() = {
    sc.shutdown() 
    this.cleanup()
    logger.info("test complete executing cleanup")
  }
}

class SslTest extends HttpClientTestTrait {

  @Before
  def setup(){
    sc = SslTest.sc
  }

  @Test
  def testSwaggerEntityHtmlRenders(): Unit = {
    if (sc == null){
      throw new RuntimeException("servlet context never set by test")
    }
    val r = httpGet(wrapSSL(s"api-docs/${sc.namespace}/swagger"), "text/html", "")
    assertEquals(200, r.statusCode)
    assertTrue(r.contentType.isDefined)
    assertEquals(s"text/html;charset=utf-8", r.contentType.get)
    assertThat("cannot find swagger url", r.body.get, containsString(s"${sc.namespace}/swagger.json"))
  }
}
