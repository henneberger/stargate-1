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
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{Config, ConfigFactory, ConfigList}
import com.typesafe.scalalogging.LazyLogging
import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.{After, Before, Test}
import stargate.service.testsupport._

trait QueryServletTest extends HttpClientTestTrait with LazyLogging {

  val url = wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}")

  @After
  def tearDown(): Unit = {
    val r = httpDelete(
      wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}"),
      "application/json",
      "{\"-match\": \"all\"}")
    if(r.statusCode != 200){
      logger.error(s"unable to delete records during teardown: ${r.statusCode}")
    }
  }

  @Before
  def setupRecord(): Unit = {
    logger.info(s"url for record setup ${url}")
    val r = httpPost(url,
      "application/json",
      """
          |{
          | "firstName": "Steve",
          | "lastName": "Mikey",
          | "addresses": {
          |    "street": "my street"
          |  }
          |}
          |""".stripMargin)
    if (r.statusCode !=  200){
      throw new RuntimeException(s"can't setup new record to query ${r.statusCode}")
    }
  }

  @Test
  def testEntityCreate(): Unit = {
    val r = httpPost(wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}"), "application/json",
      """
          |{
          |        "firstName": "Steve",
          |        "lastName": "Jacobs"
          |    }
          |""".stripMargin)
    assertEquals(r.statusCode, 200)
    assertTrue(r.contentType.isDefined)
    assertEquals(r.contentType.get, "application/json")
  }

  @Test
  def testEntityDelete(): Unit = {
     var r = httpPost(url, "application/json",
      """
          |{
          | "firstName": "Steve"
          |}
          |""".stripMargin)
    if (r.statusCode != 200){
      throw new RuntimeException(s"can't setup new record to query ${r.statusCode}")
    }

    r = httpDelete(wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}"),
      "application/json",
      "{\"-match\": [\"firstName\", \"=\", \"Steve\"]}")
    assertEquals("not able to delete record", 200, r.statusCode)
    assertTrue(r.contentType.isDefined)
    assertEquals(r.contentType.get, "application/json")

    val newUrl = wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}")
    val getQuery = ("""
                      |{
                      | "-match":["firstName","=", "Steve"]
                      |}
                      |""".stripMargin)
    val response = httpGet(newUrl, "application/json",getQuery)
    assertEquals("not able to query", 200, response.statusCode)
    assertTrue(response.contentType.isDefined)
    assertEquals(response.contentType.get, "application/json")
    logger.info(s"query response after delete is ${response.body}")
    assertTrue(response.body.isDefined)
    val resultBody = ConfigFactory.parseString( response.body.get)
    assertEquals("there were unexpected matches after delete", 0, resultBody.getConfigList("data").size)
  } 

  @Test
  def testEntityUpdate(): Unit = {
    val r = httpPut( 
      wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}"),
      "application/json",
      """
          |{
          |"-match":["firstName","=","Steve"],
          | "lastName": "Danger",
          | "addresses":{
          |   "-update":{
          |      "-match":["customers.firstName","=","Steve"],
          |      "street":"other st"}
          |      }
          |}
          |""".stripMargin)
    assertEquals(200, r.statusCode)
    assertTrue(r.contentType.isDefined)
    assertEquals("application/json", r.contentType.get)
    logger.info(s"update query response is ${r.body.get}")
    val getQuery = ("""
          |{
          | "-match":["firstName","=", "Steve"],
          |   "addresses":{},
          |     "orders":{}
          |}
          |""".stripMargin)
    val url = wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}")
    val response = httpGet(url, "application/json", getQuery)
    assertEquals(200, response.statusCode)
    assertTrue(response.contentType.isDefined)
    assertEquals( "application/json", response.contentType.get)
    assertTrue(response.body.isDefined)
    logger.info(s"query response after update is ${response.body}")
    val resultBody = ConfigFactory.parseString( response.body.get)
    val data = resultBody.getConfigList("data")
    assertEquals("total affected records is unexpected", 1, data.size())
    assertEquals("cannot find name of updated record", "Steve", data.get(0).getString("firstName"))
    assertEquals("updated street name does not match", "other st",
      data.get(0).getConfigList("addresses").get(0).getString("street"))
  }

  @Test
  def testEntityQuery(): Unit = {
    val getQuery = """{"-match":["firstName","=", "Steve"]}"""
    val url = wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}")
    val r = httpGet(url, "application/json", getQuery)
    assertEquals(200, r.statusCode)
    assertTrue(r.contentType.isDefined)
    assertEquals("application/json", r.contentType.get)
    assertTrue(r.body.isDefined)

    //validate body contents
    val resultBody = ConfigFactory.parseString(r.body.get)
    val data = resultBody.getConfigList("data")
    assertEquals("total affected records is unexpected", 1, data.size)
    assertEquals("cannot find record", "Steve", data.get(0).getString("firstName"))
    assertEquals("cannot find record", "Mikey", data.get(0).getString("lastName"))
  }

  @Test
  def testContinueQuery(): Unit = {
    var r = httpPost(url, "application/json",
            """
              |{
              | "firstName": "Steve",
              | "lastName": "James"
              |}
              |""".stripMargin)
    if (r.statusCode != 200){
      throw new RuntimeException(s"can't setup new record to query ${r.statusCode}")
    }
    val getQuery = """{"-match":["firstName","=", "Steve"],"-limit":1,"-continue":true}"""
    val queryUrl = wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}")
    val response = httpGet(queryUrl, "application/json", getQuery)
    val resultBody = ConfigFactory.parseString(response.body.get)
    val queryId: String = resultBody.getString("continue.continueId")
    r = httpGet(wrap(s"${StargateApiVersion}/api/${sc.namespace}/continueQuery/${queryId}"), "application/json", "")
    assertEquals(200, r.statusCode)
    assertTrue("content type is not defined", r.contentType.isDefined)
    assertEquals("application/json", r.contentType.get)
    assertThat("cant find user account on 2nd page", r.body.get, containsString("Steve"))
  }

  @Test
  def testStoredQuery(): Unit = {
    val getQuery = """
                     |{
                     |        "-match":{
                     |            "customerName": "Steve"
                     |        }
                     |}
                     |""".stripMargin
    val myQuery = "customerByFirstName"
    val queryUrl = wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/${myQuery}")
    val response = httpGet(queryUrl, "application/json", getQuery)
    logger.info(s"continue requested query response is ${response.body.get}")
    assertEquals(response.statusCode, 200)
    assertTrue(response.contentType.isDefined)
    assertEquals(response.contentType.get, "application/json")
    assertTrue(response.body.isDefined)
    val responseBody = ConfigFactory.parseString(response.body.get)
    assertEquals(responseBody.getConfigList("data").get(0).getString("firstName"), "Steve")
  }

  @Test
  def testEntityById(): Unit = {
    print("testing by entity id")
    val queryUrl = wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}")
    val getQuery = """{"-match":"all"}"""
    val response = httpGet(queryUrl, "application/json", getQuery)
    val all = ConfigFactory.parseString(response.body.get).getConfigList("data")
    logger.info(s"output from query response all: $all")
    val uuid = UUID.fromString(all.get(0).getString("entityId"))

    def get(): java.util.List[_ <: Config] = {
       val body = httpGet(wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}/${uuid}"), "application/json", "").body.get
       logger.info(s"body from testEntityById $body")
       ConfigFactory.parseString(body).getConfigList("data")
    }
    assertTrue("record did not match on get by id", get().get(0) == all.get(0))
    val update = httpPut(wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}/${uuid}"),
                "application/json",
                """{"firstName":"asdf"}""".stripMargin)
    assert(get().get(0) != all.get(0))
    val delete = httpDelete(wrap(s"${StargateApiVersion}/api/${sc.namespace}/entity/${sc.entity}/${uuid}"),
      "application/json", "")
    assertTrue("record was not deleted", get().isEmpty)
  }
}
