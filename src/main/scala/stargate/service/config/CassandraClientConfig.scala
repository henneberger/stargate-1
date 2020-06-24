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

import scala.beans.BeanProperty
import CassandraClientConfig._
import com.typesafe.config.Config
import scala.collection.immutable.HashMap
/**
  * properties needed to create a cassandra client
  *
  * @param cassandraContactPoints contact points used for stargate to connect to cassandra, usually 2 or 3 nodes will suffice
  * @param cassandraDataCenter datacenter to connect to. Note if a listed contact point is not in this datacenter the connection will not work
  * @param cassandraReplication number of copies of data stored (ie RF 1 means there is only a single copy of all data and losing it means losing that data permanently)
  * @param cassandraAuthProvider custom auth provider to use for Apache Cassandra. See https://docs.datastax.com/en/developer/java-driver/4.6/manual/core/authentication/ for more details
  * @param cassandraUserName when using the PlainTextProvider the user name to use to connect to Apache Cassandra
  * @param cassandraPassword when using the PlainTextAuthProvider the password to use to connect to Apache Cassandra */
case class CassandraClientConfig(
  @BeanProperty val cassandraContactPoints: List[(String, Int)],
  @BeanProperty val cassandraDataCenter: String = DEFAULT_DATACENTER,
  @BeanProperty val cassandraReplication: Map[String, Integer] = DEFAULT_REPLICATION,
  @BeanProperty val cassandraUserName: String = DEFAULT_USERNAME,
  @BeanProperty val cassandraPassword: String = DEFAULT_PASSWORD,
  @BeanProperty val cassandraAuthProvider: String = DEFAULT_AUTH_PROVIDER)


object CassandraClientConfig {

  val DEFAULT_DATACENTER:String = "datacenter1"
  val DEFAULT_REPLICATION: Map[String, Integer] = HashMap("datacenter1"->new Integer(1))
  val DEFAULT_AUTH_PROVIDER = ""
  val DEFAULT_USERNAME = "cassandra"
  val DEFAULT_PASSWORD = "cassandra"

  def parse(config: Config): CassandraClientConfig = {
    val replicationString = config.getString("replication")
    val replicationInt = replicationString.toIntOption
    def convertToReplicationMap(): Map[String, Integer] = {
      if (replicationInt.isDefined){
        HashMap(config.getString("dataCenter")-> new Integer(replicationInt.get))
      } else {
        replicationString.split(",").map(x=>x.split(":")).map(x=>(x(0).trim, new Integer(x(1).trim.toInt))).toMap[String, Integer]
      }
    }

    CassandraClientConfig(
      config.getString("contactPoints").split(",").map(_.split(":")).map(hp => (hp(0), Integer.parseInt(hp(1)))).toList,
      config.getString("dataCenter"),
      convertToReplicationMap(),
      config.getString("username"),
      config.getString("password"),
      config.getString("authProvider"))
  }
}
