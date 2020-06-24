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

import org.junit.Test
import com.typesafe.config.ConfigFactory
import org.junit.Before
import org.junit.Assert._
import org.hamcrest.CoreMatchers._

class StargateConfigTest {

  var sgConfig: StargateConfig = _

  @Before
  def setup(): Unit = {
    val config = ConfigFactory.parseResources("testsg.conf").resolve()
    sgConfig = StargateConfig.parse(config)
  }

  @Test
  def testSGConfig(){
    assertThat(sgConfig.defaultLimit, equalTo(10))
    assertThat(sgConfig.defaultTTL, equalTo(60))
    assertThat(sgConfig.stargateKeyspace, equalTo("stargate_system"))
  }

  @Test
  def testValidation(){
    assertThat(sgConfig.maxRequestSizeKB, equalTo(4096L))
    assertThat(sgConfig.maxSchemaSizeKB, equalTo(128L))
    assertThat(sgConfig.maxMutationSizeKB, equalTo(1024L))
  }

  @Test
  def testAuthConfig(){
    val authConfig = sgConfig.auth
    assertThat(authConfig.enabled, equalTo(false))
    assertThat(authConfig.passwordHash, equalTo("$2a$12$E3tbBnSsZInKlehcUt2DIuaH9XcXvzXmOozQKgai2iZlvzRQ93nHS"))
    assertThat(authConfig.user, equalTo("jjadmin"))
  }

  @Test
  def testCassandraConfigParsesCorrectly(){
     val cassConfig = sgConfig.cassandra
     assertThat(cassConfig.cassandraAuthProvider, equalTo("PlainTextAuthProvider"))
     assertThat(cassConfig.cassandraUserName, equalTo("myuser"))
     assertThat(cassConfig.cassandraPassword, equalTo("mypass"))
     assertThat(cassConfig.cassandraReplication.getOrElse("mydc", new Integer(-1)), equalTo(new Integer(7)))
     assertThat(cassConfig.cassandraContactPoints, equalTo(List(("192.168.1.2", 9042),("192.168.1.3", 9042))))
     assertThat(cassConfig.cassandraDataCenter, equalTo("mydc"))
  }

  @Test
  def testMultiDCParsesCorrectly(){
    val config = ConfigFactory.parseResources("testsg-multidc.conf").resolve()
    val sgMultiDcConfig = StargateConfig.parse(config)
    val cassConfig = sgMultiDcConfig.cassandra
    assertThat(cassConfig.cassandraReplication.getOrElse("mydc", new Integer(-1)), equalTo(new Integer(7)))
    assertThat(cassConfig.cassandraReplication.getOrElse("mydc2", new Integer(-1)), equalTo(new Integer(5)))
  }
}
