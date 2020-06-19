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
## Getting started in three easy steps.

Getting a microservice and cassandra off the ground can be a big task. Stargate makes this as easy as three steps.

**Prerequisites**: You must have `docker` installed to run stargate locally.
The last example uses `jq` to parse JSON responses on the command line - you can install this from brew on mac OS,
or from your package manager on Linux.
Minimal testing has been done on Windows, so it is currently unsupported.


1. Create an example schema configuration file
    ```
    echo 'entities {
      Todo {
          fields {
              todo: string
              isComplete: boolean
          }
      }
    }
    queryConditions: {
      Todo: [
        ["todo", "="]
      ]
    }' > stargate.conf
    ```

2. Download the stargate CLI<sup id="a1">[*](#f1)</sup>, start up local cassandra and stargate instances, then create a database named `myNamespace` from the configuration in step 1.
```sh
curl -O -L "https://github.com/datastax/stargate/releases/download/v0.3.0/stargate_0.3.0_$(uname -s)_x86_64.tar.gz"
tar -xzf ./stargate_*.tar.gz
./stargate service start --with-cassandra 
./stargate apply myNamespace stargate.conf
```
    
3. Query the database

Create a Todo:
```sh
curl -X POST "http://localhost:8080/v1/api/myNamespace/entity/Todo" \
     -H "content-type: application/json" -d'
{ 
 "todo": "Get stargate running",
 "isComplete": false
}
' > ./createResponse.out
cat ./createResponse.out
{"data":[{"entityId":"50d5b8f6-3f8e-4d60-ba28-785a1412f542","-action":"created"}]}
```

Get todos:
```sh
curl -X GET "http://localhost:8080/v1/api/myNamespace/entity/Todo" \
     -H "content-type: application/json" -d'
{ 
 "-match": "all"
}
'
```

Update todo:
```sh
todoId=$(cat ./createResponse.out | jq -r ".data[0].entityId")
curl -X PUT "http://localhost:8080/v1/api/myNamespace/entity/Todo" \
     -H "content-type: application/json" -d'
{ 
 "-match": ["entityId", "=", "'"${todoId}"'"],
 "isComplete": true
}
'
```

<br />
<span style="color:grey">

###### footnotes
<sup id="f1">*</sup> If this fails on your system, you can try downloading the appropriate CLI binary from: https://github.com/datastax/stargate/releases [↩](#a1)

</span>
