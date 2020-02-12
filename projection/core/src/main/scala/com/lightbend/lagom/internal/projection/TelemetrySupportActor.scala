package com.lightbend.lagom.internal.projection

import akka.actor.Actor
import akka.actor.Props
import com.lightbend.lagom.internal.projection.ProjectionRegistryActor.WorkerCoordinates
import com.lightbend.lagom.internal.projection.TelemetrySupportActor.Start

import scala.collection.mutable

object TelemetrySupportActor {
  def props(coordinates: WorkerCoordinates): Props =
    Props(
      new TelemetrySupportActor(
        coordinates
      )
    )

  // actor Protocol
  case class CorrelationId(id: Int)
  case class Start[Event](
      event: Event,
      correlationId: CorrelationId,
      eventTimestamp: Long
  )
  case class Success(eventClass: Class[_], correlationId: CorrelationId)

  // internal model
  case class Context(
      correlationId: CorrelationId,
      coordinates: WorkerCoordinates,
      startTimestamp: Long,
      eventTimestamp: Long
  )
}

/**
 * Keeps the context required by Cinnamon in a queue so different stages of a projection
 * may access it. The internal queue must keep data in the same order than the messages
 * traveling across the projection stream. When the projection flow filter messages this
 * actor will be able to drop items from the internal queue.
 * When the projection stream is restarted the existing incarnation fo this actor should
 * be dropped and a new incarnation must be created.
 */
class TelemetrySupportActor(coordinates: WorkerCoordinates) extends Actor {
  import TelemetrySupportActor._

  // queue of contexts observed in the projection stream. Must keep strict order.
  // The Context includes a CorrelationId because the stream may filter messages,
  // in which case we dropWhile until we see the correlationId we expected.
  val inflightContexts: mutable.Queue[Context] = mutable.Queue.empty[Context]

  override def receive: Receive = {

    case Start(event, correlationId, eventTimestamp) =>
      val ctx = Telemetry.start(correlationId, coordinates, eventTimestamp)
      inflightContexts.enqueue()

    case Success(event, correlationId) =>
      val dropped: mutable.Seq[Context] = inflightContexts.dequeueAll(_.correlationId != correlationId)
      // TODO: report the dropped Context's as filtered?
      val desired: Context              = inflightContexts.dequeue()
      Telemetry.success(desired)
  }
}

// This indirection is meant to ease instrumentation (if needed)
object Telemetry {
  import TelemetrySupportActor._
  def start(
      correlationId: CorrelationId,
      coordinates: WorkerCoordinates,
      eventTimestamp: Long
  ): Context =
    Context(
      correlationId,
      coordinates,
      now(),
      eventTimestamp
    )

  def success(context: Context): Unit = ()

  // TODO: is currentTimeMillis correct?
  def now(): Long = System.currentTimeMillis()

}
