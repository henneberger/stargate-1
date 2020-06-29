<!--
    Copyright DataStax, Inc.
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# Production

## Authentication

As of 0.2.0 authentication is very minimal and only allows setting a
username and a password hashed with [bcrypt](https://en.wikipedia.org/wiki/Bcrypt).

### Encrypting Stargate Traffic

Turning on encryption will also activate HTTP/2 as an option when connecting.
To setup SSL one just needs to set the following environment variables
and start the service:

```sh
export SG_SERVICE_SSL=true
export SG_SERVICE_SSL_CERT=/mytruststore
export SG_SERVICE_SSL_PASS=mypassword
java -jar stargate.jar
```

#### Docker Encrypting Cassandra Traffic

To use ssl with the docker image one has to mount a generated keystore
with the desired certificate:

```sh
docker run -d -e SG_SERVICE_SSL=true -e SG_SERVICE_SSL_PASS=mypassword \
-e SG_SERVICE_SSL_CERT=/mykeystore \
-v mykeystore:/mykeystore
stargate:latest
```

### Encrypting Cassandra Driver Traffic

The DataStax Java Driver that Stargate
[uses handles this](https://docs.datastax.com/en/developer/java-driver/4.6/manual/core/ssl/).

The easiest approach I've found for doing a quick test is passing the -D
flags when starting stargate The
`-Ddatastax-java-driver.advanced.ssl-engine-factory.class=DefaultSslEngineFactory`
flag actually enables ssl and the `-Djavax.net.ssl.trustStore` and
`-Djavax.net.ssl.trustStorePassword` flags are needed to
to read your certificate or trust a CA.

```sh
java -Djavax.net.ssl.trustStorePassword=mypassword \
-Djavax.net.ssl.trustStore=mytruststore \
-Ddatastax-java-driver.advanced.ssl-engine-factory.class=DefaultSslEngineFactory \
-jar stargate.jar
```

#### Docker Encrypting Cassandra Traffic

As of Stargate 3.2.2 we can pass -D flags to the container through the
SG_SERVICE_JAVA_ARGS environment variable. Combined with the use of a volumes this
is an easy way to encrypt traffic in docker.

```sh
docker run -d \
-e SG_SERVICE_JAVA_ARGS="-Djavax.net.ssl.trustStorePassword=mypassword -Djavax.net.ssl.trustStore=/mytruststore -Ddatastax-java-driver.advanced.ssl-engine-factory.class=DefaultSslEngineFactory" \
```

### Enabling Auth

First generate a password that is hashed by bcrypt. There are several ways
to do this but we provide a simple utility command in
stargate with `stargate encrypt`

```sh
stargate encrypt
$enter your password
********
$enter your password again
********
```

Then set the following environment variables on startup
(please generate your own hash)

```sh
export SG_SERVICE_AUTH_ENABLED=true
export SG_SERVICE_AUTH_USER=admin
export SG_SERVICE_AUTH_PASS_HASH='$2a$12$BW57tXiKKTl.nf853gZWju2QpkfhYXRDYEw5ucEF6yCKgSw3QuaTG'
```

## Authenticating with Cassandra

Currently an alpha feature we do provide a basic way to authenticate
against Apache Cassandra and DataStax Enterprise:

Then set the following environment variables on startup (note password is not encrypted):

```sh
export SG_CASS_USER=myuser
export SG_CASS_PASS=mypass
export SG_CASS_AUTH_PROVIDER=PlainTextAuthProvider
```

If you'd rather you can edit the following fields in defaults.conf

```hocon
cassandra: {
    contactPoints: "localhost:9042"
    dataCenter: datacenter1
    replication: 1
    username: "myuser"
    password: "mypass"
    authProvider: "PlainTextAuthProvider"
}
```

Finally, assuming you've made the jar available to stargate you can add your own
custom auth provider as long as it has a no argument constructor which is
covered [in better detail here](https://docs.datastax.com/en/developer/java-driver/4.6/manual/core/authentication/)

### Auth With Docker, no proper hostname and a SSL Self Signed Cert

Since this combination is a bit tricky to spin up, I've provided an example
using an already generated truststore.

```ssh
docker run \
-e SG_CASS_CONTACT_POINTS=172.17.0.2:9042 -e SG_CASS_USER=cassandra \
-e SG_CASS_PASS=cassandra -e SG_CASS_AUTH_PROVIDER=PlainTextAuthProvider \
-e SG_SERVICE_JAVA_ARGS='-Djavax.net.ssl.trustStorePassword=abcd123 -Djavax.net.ssl.trustStore=/usr/app/truststore -Ddatastax-java-driver.advanced.ssl-engine-factory.class=DefaultSslEngineFactory -Ddatastax-java-driver.advanced.ssl-engine-factory.hostname-validation=false' \
-v $(pwd)/truststore:/usr/app/truststore datastax/stargate:v3.2.2
```

## Kubernetes

Note: Tested with Helm 3.2.1

The helm chart can be downloaded from our source [here](kube/charts/stargate).
Assuming you've checked out the entire tree and are in the base
directory it can be installed like so:

```sh
helm install cassandra incubator/cassandra
helm install stargate-deploy ./kube/charts/stargate
```

The default username and password for stargate is `admin` and `sgAdmin1234`

### Custom contact points, data center and RF for new namespaces

```sh
helm install stargate-deploy ./kube/charts/stargate \
--set stargate.cassandraContactPoints='myhost1:9042,myhost2:9042' \
--set stargate.cassandraDataCenter=myDc --set stargate.cassandraReplicationFactor=3
```

### Custom credentials for cassandra

```sh
helm install stargate-deploy ./kube/charts/stargate \
--set stargate.cassandraAuthUser=myuser --set stargate.cassandraAuthPass=mypass
```

### Set username and password

```sh
helm install stargate-deploy ./kube/charts/stargate \
--set stargate.authUser=myUser \
--set stargate.authPasswordHash=$2a$12$E3tbBnSsZInKlehcUt2DIuaH9XcXvzXmOozQKgai2iZlvzRQ93nHS 
```
