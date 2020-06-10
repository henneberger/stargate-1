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

package stargate

object keywords {

  val KEYWORD_PREFIX = "-"

  object query {
    val GET: String = KEYWORD_PREFIX + config.query.op.GET
    val INCLUDE: String = KEYWORD_PREFIX + "include"
    val MATCH_ALL: String = "all"
  }

  object mutation {
    val CREATE: String = KEYWORD_PREFIX + config.query.op.CREATE
    val UPDATE: String = KEYWORD_PREFIX + config.query.op.UPDATE
    val MATCH: String = KEYWORD_PREFIX + config.query.op.MATCH
    val DELETE: String = KEYWORD_PREFIX + config.query.op.DELETE
  }

  object relation {
    val LINK: String = KEYWORD_PREFIX + "link"
    val UNLINK: String = KEYWORD_PREFIX + "unlink"
    val REPLACE: String = KEYWORD_PREFIX + "replace"
  }

  object pagination {
    val LIMIT: String = KEYWORD_PREFIX + "limit"
    val CONTINUE: String = KEYWORD_PREFIX + "continue"
    val TTL: String = KEYWORD_PREFIX + "ttl"
  }

  object response {
    val DATA: String = "data"
    val ACTION: String = KEYWORD_PREFIX + "action"
    val ACTION_CREATE: String = "created"
    val ACTION_UPDATE: String = "updated"
    val ACTION_DELETE: String = "deleted"
    val RELATION: String = KEYWORD_PREFIX + "relation"
    val RELATION_LINK: String = "link"
    val RELATION_UNLINK: String = "unlink"

    object continue {
      val ROOT: String = "continue"
      val CONTINUE_ID: String = "continueId"
      val ENTITIES: String = "entities"
    }
  }

  // should probably move keywords to top-level module or somewhere else if going to have both config/schema and runtime keywords
  object config {
    val ENTITIES: String = "entities"
    val QUERIES: String = "queries"
    val QUERY_CONDITIONS: String = "queryConditions"

    object entity {
      val FIELDS: String = "fields"
      val RELATIONS: String = "relations"
      val RELATION_TYPE: String = "type"
      val RELATION_INVERSE: String = "inverse"
    }

    object query {

      object op {
        val GET: String = "get"
        val CREATE: String = "create"
        val MATCH: String = "match"
        val UPDATE: String = "update"
        val DELETE: String = "delete"
      }
      val crudOps: Set[String] = Set(op.CREATE, op.GET, op.UPDATE, op.DELETE)

      object relation {
        val OP_LINK_CONFIG: String = "link"
        val OP_UNLINK_CONFIG: String = "unlink"
        val OP_REPLACE_CONFIG: String = "replace"
      }
    }
  }
}
