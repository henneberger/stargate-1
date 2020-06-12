package stargate.query

import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import stargate.model.{OutputModel, ScalarCondition}
import stargate.query
import stargate.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}

package object ramp {

  object TransactionState extends Enumeration {
    class Value(val name: String, val asInt: Int) extends super.Val(name)
    val IN_PROGRESS = new Value("IN_PROGRESS", 1)
    val SUCCESS = new Value("SUCCESS", 0)
    val FAILED = new Value("FAILED", 2)

    val ints: Map[Int, Value] = this.values.iterator.map(state => {
      val stateValue = state.asInstanceOf[Value]
      (stateValue.asInt, stateValue)
    }).toMap
    def toInt(state: Value): Int = state.asInt
    def fromInt(int: Int): Value = ints(int)
  }

  type GetTransactionState = UUID => Future[TransactionState.Value]
  type SetTransactionState = (UUID, TransactionState.Value) => Future[Unit]

  case class Context(model: OutputModel, getState: GetTransactionState, setState: SetTransactionState, session: CqlSession,executor: ExecutionContext) {
    val queryContext = query.Context(model, session, executor)
  }

  def matchEntities(context: Context, transactionId: UUID, rootEntityName: String, relationPath: List[String], conditions: List[ScalarCondition[Object]]): AsyncList[Map[String,Object]] = {
    val potentialIds = query.matchEntities(context.queryContext, rootEntityName, relationPath, conditions).dedupe(context.executor)
    val entities = potentialIds.map(id => {

    })
  }

}
