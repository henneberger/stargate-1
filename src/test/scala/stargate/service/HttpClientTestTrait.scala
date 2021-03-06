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

import org.json4s.native.Serialization
import stargate.service.testsupport.ServletContext
import stargate.service.testsupport._

trait HttpClientTestTrait {

  var sc: ServletContext = MergedServletTest.sc

  /**
   * wrap sets up the proper location of the http test server
   *
   * @param url base url to use. Do not start with a /
   */
  def wrap(url: String) : String = {
    s"http://localhost:9090/${url}"
  }

  /**
   * wrapSSL sets up the proper location of the ssl server
   *
   * @param url base url to use. Do not start with a /
   */
  def wrapSSL(url: String) : String = {
    s"https://localhost:9092/${url}"
  }
}
