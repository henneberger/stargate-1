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

# Stargate

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
![build](https://github.com/datastax/stargate/workflows/Docker%20Image%20CI/badge.svg)
![stage](https://img.shields.io/badge/stage-alpha-orange)

Stargate is an ORM and service layer for Apache Cassandra and DataStax
Enterprise.  Given a logical data model of entities, relationships, and
queries Stargate will generate the appropriate data model in
Apache Cassandra or DataStax Enterprise.

## Benefits

* No CQL required. Work directly with a RESTful, microservice-friendly API.
* Allows you to stay focused on building your app and its queries,
rather than how you want Cassandra to store your data.
* Stargate is fully Cloud Native which means you can easily run Stargate on
EKS, GCE, or your own local Kubernetes cluster. However,
you don’t need Kubernetes to run Stargate.
* Relax: Think about how you want to query your data, rather
than how you want to store it.
* Low Code: Get a well tuned and optimized
http REST endpoint and driver with no code

## Stargate is built for

People that need to ship now and ship fast. Such as:

* Front end developers building something that needs to scale
* Small consulting shops trying to deliver to several customers on a deadline
* New to NoSQL user just trying to make sense of it all

## Getting Started

Ready to give it a try?

* [Getting started in 3 quick steps](docs/getting_started.md)
* [Build a Todo application database in 5 minutes](docs/getting_started_todo_app.md)

## Understanding Stargate

* [Schema Definition](docs/schema_definition.md)
* [CRUD Operations](docs/crud.md)
* [Deploy](docs/deploy.md)
* [FAQ](docs/faq.md)

## Features

* **Cloud native.** Scale-out and in with workload demand. Be highly available
and resilient to failure. Have quick response times across all channels.
* **It just works.** You give us a logical data model and Stargate gives you
a deployable microservice. Stargate handles the database and schema.
* **CRUD made easy.** Stargate exposes a fully featured CRUD API. Create, update,
and delete records in cassandra with ease.
* **Advanced Cassandra support.** Trust that your database can scale
out horizontally to meet increasing workload demand.

### Command line utility

Ok now you're sold, go download the binaries for
[Linux and Windows here](https://github.com/datastax/stargate/releases/latest)
and for Mac we suggest using homebrew

```sh
brew tap foundev/stargate
brew install stargate
```

### Production

We provide a basic references for the things you may want to
enable in production [here](docs/production.md)

## Contributing

Requirements:

* [OpenJDK 11](https://adoptopenjdk.net/releases.html)
* [Apache Maven](https://maven.apache.org/)
* [Docker](https://www.docker.com) if you want to build the image.
* [Apache Cassandra 3.11](https://cassandra.apache.org/) or [DSE 6.8](https://downloads.datastax.com/#enterprise)

Run the following:

```sh
git clone git@github.com:datastax/stargate
mvn compile test
mvn exec:java #with DSE or Apache Cassandra running in the background
```

If all this is working satisfactory you are ready to develop new features for Stargate.

### Debugging

Changing the `stargate` logger from `INFO` to `TRACE` will give a per
request headers and output, not appropriate to run very long in production
but should be fine in a dev context.
See the following example:

```xml
<logger name="stargate" level="info" additivity="false">
```

to

```xml
<logger name="stargate" level="trace" additivity="false">
```
