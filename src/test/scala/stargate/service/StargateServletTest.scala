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

import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}

import org.junit.Assert._
import org.junit.Test
import stargate.KeyspaceRegistry
import stargate.service.testsupport._

import scala.io.Source

trait StargateServletTest extends HttpClientTestTrait with KeyspaceRegistry {

  @Test
  def testNamespaceCreate(): Unit = {
    val newNamespace = this.registerKeyspace(s"testCreate${sc.rand.nextInt(2000)}")
    var r = httpPost(wrap(s"${StargateApiVersion}/api/${newNamespace}/schema"),
      "application/hocon",
      Source.fromResource("schema.conf").getLines().mkString("\n"))
    assertEquals(200, r.statusCode)
    assertTrue(r.contentType.isDefined)
    assertEquals("application/json", r.contentType.get)

    val getQuery = """{"-match":["firstName","=", "Steve"]}"""
    val url = wrap(s"${StargateApiVersion}/api/${newNamespace}/entity/${sc.entity}")
    r = httpGet(url, "application/json", getQuery)
    //validate I can hit the end point even if there are no results
    assertEquals(200, r.statusCode)
  }

  @Test
  def testDeleteNamespace(): Unit = {
    val newNamespace = this.registerKeyspace(s"testCreate${sc.rand.nextInt(2000)}")
    var r = httpPost(wrap(s"${StargateApiVersion}/api/${newNamespace}/schema"), "application/hocon", Source.fromResource("schema.conf").getLines().mkString("\n"))
    assertEquals("uanble to post schema", 200, r.statusCode)
    //deleting right after creating can cause a race, better to just retry every 100 milliseconds 
    def deleteTest (): Int = {
      var statusCode: Int = -1
        for ( a <- 0 to 5){
          Thread.sleep(100)
          r = httpDelete(wrap(s"${StargateApiVersion}/api/${newNamespace}/schema"), "application/json", "")
          statusCode = r.statusCode
          if (statusCode == 200){
            //quit early
            return statusCode;
          } else {
            println(s"WARN retrying delete operation because statusCode returned $statusCode")
          }
        }
        return statusCode
    }
    assertEquals("unable to delete namespace", 200, deleteTest())
    //validate there is no endpoint to hit
    val getQuery = """{"-match":["firstName","=", "Steve"]}"""
    val url = wrap(s"${StargateApiVersion}/api/${newNamespace}/entity/${sc.entity}")
    r = httpGet(url, "application/json", getQuery)
    //should get bad gateway with missing namespace
    assertEquals("unexpected successed, endpoint was deleted and this should fail", 502, r.statusCode )
  }

  @Test
  def testValidateSchema(): Unit = {
    val r = httpPost(
      wrap(s"${StargateApiVersion}/api/validate"),
      "application/hocon",
      Source.fromResource("schema.conf").getLines().mkString("\n"))
    assertEquals(200, r.statusCode)
  }
}
