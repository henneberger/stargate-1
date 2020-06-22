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
import org.junit.Assert._
import org.junit.Test
import stargate.service.testsupport._

trait SwaggerServletTest extends HttpClientTestTrait {
  
  @Test
  def testSwaggerEntityHtmlRenders(): Unit = {
    val r = httpGet(wrap(s"api-docs/${sc.namespace}/swagger"), "text/html", "")
    assertEquals(200, r.statusCode)
    assertTrue(r.contentType.isDefined)
    assertEquals(s"text/html;charset=utf-8", r.contentType.get)
    assertThat("cannot find swagger url", r.body.get, containsString(s"${sc.namespace}/swagger.json"))
  }

  @Test
  def testSwaggerEntityHtmlRendersWithTrailingSlash(): Unit = {
    val r = httpGet(wrap(s"api-docs/${sc.namespace}/swagger/"), "text/html", "")
    assertEquals(200, r.statusCode)
    assertTrue(r.contentType.isDefined)
    assertEquals(s"text/html;charset=utf-8", r.contentType.get)
    assertThat("cannot find swagger url", r.body.get, containsString(s"${sc.namespace}/swagger.json"))
  }

  @Test
  def testSwaggerEntityJsonRenders(): Unit = {
    val r = httpGet(wrap(s"api-docs/${sc.namespace}/swagger.json"), "application/json", "")
    assertEquals(200, r.statusCode)
    assertTrue(r.contentType.isDefined)
    assertEquals("application/json", r.contentType.get)
    assertThat("namespace missing", r.body.get, containsString(sc.namespace))
    assertThat("entity missing", r.body.get, containsString(sc.entity))
  }

  @Test
  def testSwaggerNamespacesRenders(): Unit = {
    val r = httpGet(wrap(s"api-docs/swagger"), "text/html", "")
    assertEquals(200, r.statusCode)
    assertTrue(r.contentType.isDefined)
    assertEquals(s"text/html;charset=utf-8", r.contentType.get)
    assertThat("cannot find swagger url", r.body.get, containsString("swagger.json"))
  }
}
