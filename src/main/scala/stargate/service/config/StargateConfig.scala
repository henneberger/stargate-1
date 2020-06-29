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
package stargate.service.config

import com.typesafe.config.Config

import scala.beans.BeanProperty

/**
  * the parsed combination of stargate-docker.conf and defaults.conf
  *
  * @param httpPort port to run the stargate http service on
  * @param defaultTTL default ttl for continuing paging requests. Raising this means pages can be attempted again much later, but there maybe more ram and cpu used to manage paging sessions
  * @param defaultLimit default page size limit for all requests
  * @param maxSchemaSizeKB max allowed size in KB for schema json posted to the ${namespace}/create and validate links
  * @param maxRequestSizeKB max allowed request size in KB for all http requests
  * @param maxMutationSizeKB max allowed mutation size in KB for all create entity requests ${namespace}/query/entitity/${entityName}/ POST
  * @param stargateKeyspace cassandra keyspace for persisting stargate's internal state (e.g. active schemas)
  * @param cassandra provides the Apache Cassandra client configuration
  * @param auth provides the server authentication configuration
  */
final case class StargateConfig(
    @BeanProperty val httpPort: Int,
    @BeanProperty val defaultTTL: Int,
    @BeanProperty val defaultLimit: Int,
    @BeanProperty val maxSchemaSizeKB: Long,
    @BeanProperty val maxRequestSizeKB: Long,
    @BeanProperty val maxMutationSizeKB: Long,
    @BeanProperty val stargateKeyspace: String,
    @BeanProperty val cassandra: CassandraClientConfig,
    @BeanProperty val auth: AuthConfig

) {

  /**
    * returns the maximum allowed schema size in bytes
    */
  val maxSchemaSize: Long = maxSchemaSizeKB * 1024

  /**
    * returns the maximum allowed mutation size in bytes
    */
  val maxMutationSize: Long = maxMutationSizeKB * 1024

  /**
    * returns the maximum allowed http request size in bytes
    */
  val maxRequestSize: Long = maxRequestSizeKB * 1024
}

object StargateConfig {

  def parse(config: Config): StargateConfig = {
    StargateConfig(
      config.getInt("http.port"),
      config.getInt("defaultTTL"),
      config.getInt("defaultLimit"),
      config.getLong("validation.maxSchemaSizeKB"),
      config.getLong("validation.maxRequestSizeKB"),
      config.getLong("validation.maxMutationSizeKB"),
      config.getString("stargateKeyspace"),
      CassandraClientConfig.parse(config.getConfig("cassandra")),
      AuthConfig.parse(config.getConfig("auth"))
    )
  }
}
