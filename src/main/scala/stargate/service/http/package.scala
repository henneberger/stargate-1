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

import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest

/**
 * provides helper methods for the http service
 */
package object http {

  /**
   *
   * validates the total request size is not above a certain amount in bytes.
   *
   * @param contentLength is the length in bytes of the http request.
   * @param maxRequestSize is the max length in bytes allowed by the http request.
   */
  def validateRequestSize(contentLength: Long, maxRequestSize: Long): Unit = {
    if (contentLength > maxRequestSize){
      throw MaximumRequestSizeException(contentLength, maxRequestSize)
    }
  }

  /**
   * validates the schema file submitted is not bigger than a specified size
   *
   * @param contentLength content length of the http request in bytes
   * @param maxSchemaSize max size of the http request when a schema file is submitted
   */
  def validateSchemaSize(contentLength: Long, maxSchemaSize: Long): Unit= {
    if (contentLength > maxSchemaSize){
      throw SchemaToLargeException(contentLength, maxSchemaSize)
    }
  }

  /**
   *
   * validates the operation is PUT or POST and there that the content length does not exceed
   * the max mutation size.
   * 
   * @param op http method used 
   * @param contentLength how long the http request is
   * @param maxMutationSize the limit in bytes of the mutation request send via http
   */
  def validateMutation(op: String, contentLength: Long, maxMutationSize: Long): Unit ={
    if ((op == "PUT" || op == "POST") && contentLength > maxMutationSize){
      throw MaxMutationSizeException(contentLength, maxMutationSize)
    }
  }

  val hoconType = "application/hocon"
  val jsonType = "application/json"
  val jsonTypeEncoding = "charset=utf-8"
  val pathRegex: Pattern = Pattern.compile("//")

  /**
    *
    * helper method for servlet calls validateHoconContentType
    * @param req from an http servlet
    */
  def validateHoconHeader(req: HttpServletRequest): Unit = {
    val contentType = req.getContentType
    validateHoconContentType(contentType)
  }

  /**
    *
    * accepts any case of 
    * application/hocon
    * @param contentType content-type from an http request header
    */
  def validateHoconContentType(contentType: String): Unit = {
    if (contentType == null ){
      throw new InvalidContentTypeException(hoconType, contentType)
    }
    if (contentType.toLowerCase() != hoconType) {
      throw new InvalidContentTypeException(hoconType, contentType)
    }
  }

  /**
    * helper method for servlet calls validateJsonContentType
    * @param req from an http servlet
    */
  def validateJsonContentHeader(req: HttpServletRequest) : Unit = {
    val contentType = req.getContentType
    validateJsonContentType(contentType)
  }

  /**
    * accepts any case of 
    * application/json
    * application/json;charset=utf-8 
    * application/json; charset=utf-8 
    *
    * @param contentType content-type from an http request header
    */
  def validateJsonContentType(contentType: String): Unit = {
    if (contentType == null){
      throw InvalidContentTypeException(jsonType, contentType)
    }
    val tokens = contentType.toLowerCase().split(";")
    if (tokens.length == 1){
      if (tokens(0).trim() != jsonType){
        throw InvalidContentTypeException(jsonType, contentType)
      }
    } else {
      if (tokens(0).trim() != jsonType && tokens(1).trim() != jsonTypeEncoding){
        throw InvalidContentTypeException(jsonType, contentType)
      }
    }
  }

  /**
   * based on idea from spring boot, to help prevent escaping out of url string and accessing another path
   * precompile the regex above because its compiled every time on String.replaceAll()
   * @param path serverPath to sanitize
   * @return string with all of the // replaced with /
   */
  def sanitizePath(path: String): String = {
    pathRegex.matcher(path).replaceAll("/")
  }

  /** 
   *  InvalidContentTypeException is designed to be used when the wrong content type is sent in an
   *  http request
   */
  case class InvalidContentTypeException(expectedContentType: String, contentType: String)
  extends Exception(s"Expected $expectedContentType but was $contentType"){}
}

