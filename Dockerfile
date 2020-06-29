#   Copyright DataStax, Inc.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


#
## Build stage
#
FROM maven:3.6-jdk-11 AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package -DskipTests
ENV SG_SERVICE_JAVA_ARGS "-Dtest=1"
#
## Package stage
#
FROM openjdk:11-jre-slim

COPY --from=build /home/app/target ./stargate
COPY --from=build /home/app/src/main/webapp ./stargate/src/main/webapp

WORKDIR ./stargate
RUN cp ./classes/logback-prod.xml ./classes/logback.xml

CMD bash -c "java $SG_SERVICE_JAVA_ARGS -jar ./stargate.jar"
