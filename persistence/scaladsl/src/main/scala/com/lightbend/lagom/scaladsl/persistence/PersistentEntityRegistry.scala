/*
 * Copyright (C) Lightbend Inc. <https://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence

import akka.persistence.query.Offset
import akka.stream.scaladsl
import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.query.EventEnvelope

import scala.reflect.ClassTag

/**
 * At system startup all [[PersistentEntity]] classes must be registered here
 * with [[PersistentEntityRegistry#register]].
 *
 * Later, [[com.lightbend.lagom.scaladsl.persistence.PersistentEntityRef]] can be
 * retrieved with [[PersistentEntityRegistry#refFor]].
 * Commands are sent to a [[com.lightbend.lagom.scaladsl.persistence.PersistentEntity]]
 * using a `PersistentEntityRef`.
 */
trait PersistentEntityRegistry {

  /**
   * At system startup all [[com.lightbend.lagom.scaladsl.persistence.PersistentEntity]]
   * classes must be registered with this method.
   *
   * The `entityFactory` will be called when a new entity instance is to be created.
   * That will happen in another thread, so the `entityFactory` must be thread-safe, e.g.
   * not close over shared mutable state that is not thread-safe.
   */
  def register(entityFactory: => PersistentEntity): Unit

  /**
   * Retrieve a [[com.lightbend.lagom.scaladsl.persistence.PersistentEntityRef]] for a
   * given [[com.lightbend.lagom.scaladsl.persistence.PersistentEntity]] class
   * and identifier. Commands are sent to a `PersistentEntity` using a `PersistentEntityRef`.
   */
  def refFor[P <: PersistentEntity: ClassTag](entityId: String): PersistentEntityRef[P#Command]

  /**
   * A stream of the persistent events that have the given `aggregateTag`, e.g.
   * all persistent events of all `Order` entities.
   *
   * The type of the offset is journal dependent, some journals use time-based
   * UUID offsets, while others use sequence numbers. The passed in `fromOffset`
   * must either be [[akka.persistence.query.NoOffset]], or an offset that has previously been produced
   * by this journal.
   *
   * The stream will begin with events starting ''after'' `fromOffset`.
   * To resume an event stream, store the `Offset` corresponding to the most
   * recently processed `Event`, and pass that back as the value for
   * `fromOffset` to start the stream from events following that one.
   *
   * @throws IllegalArgumentException If the `fromOffset` type is not supported
   *   by this journal.
   */
  def eventStream[Event <: AggregateEvent[Event]](
      aggregateTag: AggregateEventTag[Event],
      fromOffset: Offset
  ): scaladsl.Source[EventStreamElement[Event], NotUsed]

  @InternalApi
  private[lagom] def eventEnvelopeStream[Event <: AggregateEvent[Event]](
      aggregateTag: AggregateEventTag[Event],
      fromOffset: Offset
  ): scaladsl.Source[EventEnvelope, NotUsed]
}
