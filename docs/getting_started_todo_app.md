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
# What is Stargate?

Stargate is a tool that quickly creates a microservice driven database with a logical data model. It makes querying and managing relationships easy.

This tutorial will walk you through specifying a data model and defining access patterns. By the end, you’ll have fully functioning API endpoints to test!

##### What you need

You will need both `docker` and the stargate CLI to run stargate locally.
To download the stargate CLI run:
```sh
curl -O -L "https://github.com/datastax/stargate/releases/download/v0.3.0/stargate_0.3.0_$(uname -s)_x86_64.tar.gz"
tar -xzf ./stargate_*.tar.gz
```
If this fails on your system, you can try downloading the appropriate CLI binary from: https://github.com/datastax/stargate/releases

# 1. Start the service

Before we can create and query our new stargate database, we'll start local docker containers for cassandra and the stargate service:
```sh
./stargate service start --with-cassandra
```

# 2. Design your database

The configuration file for your database is a logical data model that specifies your entities, indices, and queries.
First, we will look at the entities section of the configuration.
The example below defines an entity named `Todo` and gives it two fields: `title`, and `isComplete`.

1. Copy the following configuration to a file `./todo.conf`
    ```hocon
    entities {
        Todo {
            fields {
                title: string
                isComplete: boolean
            }
        }
    }
    ```
   Then create your database named `test` with the stargate backend by running: 
   ```sh
   ./stargate apply test ./todo.conf
   ```
   
2. Create a todo entry

    Out of the box, stargate gives you create, get, update, and delete functionality on each entity. Below is an example of a create statement.
    ```sh
    curl -X POST "http://localhost:8080/v1/api/test/entity/Todo" \
         -H "content-type: application/json" -d'
    { 
      "title": "Create a Stargate Database",
      "isComplete": false
    }
    '
    ```
    Now let’s check that the database has our change
    ```sh
    curl -X GET "http://localhost:8080/v1/api/test/query/entity/Todo" \
         -H "content-type: application/json" -d'
    { 
      "-match": "all"
    }
    '
    ```
    
# 3. Add a Relationship
Relationships are what connect the data in one entity to another. They're similar to joins in relational databases like MySQL. Let’s create a relationship to keep track of which user creates the todo.

A relationship is defined on both entities and they are linked together using an inverse reference. This allows us to link together entities and query them from either side.
```hocon
entities {
    Todo {
        fields {
            title: string
            isComplete: boolean
        }
        relations {
            user { type: User, inverse: todos }
        }
    }
    User {
        fields {
            username: string
        }
        relations {
            todos { type: Todo, inverse: user }
        }
    }
}
```

# 4. Add a custom index

The `queryConditions` section of the configuration determines what indices will be created by specifying how you want to query the data.
For this example, we want to retrieve Todos by exact match on the user’s username. We use a dot notation to access an entity through a relationship.

You can copy and paste this section at the bottom of your configuration file, after the entities section.
```hocon
queryConditions: {
    Todo: [
        ["user.username", "="]
    ]
}
```

Finally, recreate your `test` database with the new configuration by running:
```sh
./stargate apply test ./todo.conf
```

Now we’re ready to create and then query our first Todos by User. 

1. First we need to create a new user:
    ```sh
    curl -X POST "http://localhost:8080/v1/api/test/entity/User" \
         -H "content-type: application/json" -d'
    { 
     "username": "John Doe"
    }
    '
    ```
   
2. Next, we will create a new Todo and link it to our existing User.  The "-match" flag is specified as a list of (field, comparison, argument) triples,
    and any Users that match will be linked to the new parent Todo.
    ```sh
    curl -X POST "http://localhost:8080/v1/api/test/entity/Todo" \
         -H "content-type: application/json" -d'
    { 
     "title": "Create a Relation",
     "isComplete": false,
     "user": {
        "-match": ["username", "=", "John Doe"]
     }
    }
    '
    ```

3. Now we can retrieve Todos by username. 
    Once again, we use the "-match" flag with a list of (field, comparison, argument) triples.  This selects a subset of Todos to return.
    Because "user" is also included in the payload, we will also return any related Users nested under their parent Todos.
    And if those users had even more relations you wanted to traverse, you would list those in the currently empty json object following "user".
    ```sh
    curl -X GET "http://localhost:8080/v1/api/test/entity/Todo" \
         -H "content-type: application/json" -d'
    {
      "-match":["user.username", "=", "John Doe"], 
      "user": {}
    }
    '
    ```
    
    Example response:
    ```sh
    {
        "data": [{
            "entityId": "b3298af9-2e36-4e7f-8415-688aa4924183",
            "isComplete": false,
            "title": "Create a Relation",
            "user": [{
                "entityId": "268e0cf2-b896-42ec-98b1-b91c085c7ffd",
                "username": "John Doe"
            }]
        }]
    }
    ```

4. Update the todo to complete.
    Now we can update the todo with a new status. We do that with a PUT request
    ```sh
    curl -X PUT "http://localhost:8080/v1/api/test/entity/Todo" \
         -H "content-type: application/json" -d'
    {
      "-match":["user.username", "=", "John Doe"], 
      "isComplete": true
     }
    }
    '
    ```

# Congrats! You’re ready to build an application with your new database! 
## Next Steps
Once you’re ready to go to production, you can check out our [Deployment Guide](docs/deploy.md)
For more information about the features of Stargate, check out our [Docs](docs/)
## Recap
This is the final todo config that we used for our database.

todo.conf
```hocon
entities {
    Todo {
        fields {
            title: string
            isComplete: boolean
        }
        relations {
            user { type: User, inverse: todos }
        }
    }
    User {
        fields {
            username: string
        }
        relations {
            todos { type: Todo, inverse: user }
        }
    }
}
queryConditions: {
    Todo: [
        ["user.username", "="]
    ]
}
```
