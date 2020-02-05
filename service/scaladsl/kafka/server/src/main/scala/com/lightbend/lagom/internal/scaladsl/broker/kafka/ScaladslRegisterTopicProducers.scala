/*
 * Copyright (C) Lightbend Inc. <https://www.lightbend.com>
 */

package com.lightbend.lagom.internal.scaladsl.broker.kafka

import akka.actor.ActorSystem
import akka.persistence.query.Offset
import akka.stream.Materializer
import com.lightbend.internal.broker.DelegatedTopicProducer
import com.lightbend.internal.broker.TaggedInternalTopic
import com.lightbend.internal.broker.TaggedOffsetTopicProducer
import com.lightbend.lagom.internal.broker.kafka.ClassicLagomEventStreamFactory
import com.lightbend.lagom.internal.broker.kafka.DelegatedEventStreamFactory
import com.lightbend.lagom.internal.broker.kafka.EventStreamFactory
import com.lightbend.lagom.internal.broker.kafka.KafkaConfig
import com.lightbend.lagom.internal.broker.kafka.Producer
import com.lightbend.lagom.internal.projection.ProjectionRegistry
import com.lightbend.lagom.internal.scaladsl.api.broker.TopicFactory
import com.lightbend.lagom.scaladsl.api.Descriptor.TopicCall
import com.lightbend.lagom.scaladsl.api.ServiceInfo
import com.lightbend.lagom.scaladsl.api.ServiceLocator
import com.lightbend.lagom.scaladsl.api.ServiceSupport.ScalaMethodTopic
import com.lightbend.lagom.scaladsl.api.broker.Topic
import com.lightbend.lagom.scaladsl.api.broker.kafka.KafkaProperties
import com.lightbend.lagom.scaladsl.persistence.AggregateEvent
import com.lightbend.lagom.scaladsl.server.LagomServer
import com.lightbend.lagom.scaladsl.server.LagomServiceBinding
import com.lightbend.lagom.spi.persistence.OffsetStore
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

class ScaladslRegisterTopicProducers[BrokerMessage, Event <: AggregateEvent[Event]](
    lagomServer: LagomServer,
    topicFactory: TopicFactory,
    info: ServiceInfo,
    actorSystem: ActorSystem,
    offsetStore: OffsetStore,
    serviceLocator: ServiceLocator,
    projectionRegistryImpl: ProjectionRegistry
)(implicit ec: ExecutionContext, mat: Materializer) {
  private val log         = LoggerFactory.getLogger(classOf[ScaladslRegisterTopicProducers[_, _]])
  private val kafkaConfig = KafkaConfig(actorSystem.settings.config)

  // Goes through the services' descriptors and publishes the streams registered in
  // each of the service's topic method implementation.

  val service: LagomServiceBinding[_] = lagomServer.serviceBinding
  for {
    tc <- service.descriptor.topics
    topicCall = tc.asInstanceOf[TopicCall[BrokerMessage]]
  } {
    topicCall.topicHolder match {
      case holder: ScalaMethodTopic[BrokerMessage] =>
        // `topicProducer` is the user-provided method (implemented in a ServiceImp). Currently only
        // `TaggedOffsetTopicProducer` is supported. `TaggedOffsetTopicProducer` wraps the collection
        // of `tags` to fetch and the a factory:
        //    (tag, fromOffset) => Source[(Message, Offset), _]
        val topicProducer: AnyRef  = holder.method.invoke(service.service)
        val topicId: Topic.TopicId = topicCall.topicId

        // the `topicFactory` creates broker-specific producers to implement the `topicCall`
        // provided by the user
        topicFactory.create(topicCall) match {
          case _: ScaladslKafkaTopic[BrokerMessage] =>
            topicProducer match {
              case tagged: TaggedInternalTopic[BrokerMessage, Event] =>
                val tags = tagged.tags

                val eventStreamFactory: EventStreamFactory[BrokerMessage] =
                  tagged match {
                    case producer: DelegatedTopicProducer[BrokerMessage, Event] =>
                      DelegatedEventStreamFactory((tag, offset: Offset) =>
                        tags.find(_.tag == tag) match {
                          case Some(aggregateTag) =>
                            producer.persistentEntityRegistry.eventEnvelopeStream(aggregateTag, offset)
                          case None => throw new RuntimeException("Unknown tag: " + tag)
                        }
                      )
                    case producer: TaggedOffsetTopicProducer[BrokerMessage, Event] =>
                      ClassicLagomEventStreamFactory((tag, offset: Offset) =>
                        tags.find(_.tag == tag) match {
                          case Some(aggregateTag) =>
                            producer.readSideStream(aggregateTag, offset)
                          case None => throw new RuntimeException("Unknown tag: " + tag)
                        }
                      )
                  }

                val partitionKeyStrategy: Option[BrokerMessage => String] = {
                  topicCall.properties.get(KafkaProperties.partitionKeyStrategy).map { pks => message =>
                    pks.computePartitionKey(message)
                  }
                }

                Producer.startTaggedOffsetProducer(
                  actorSystem,
                  tags.map(_.tag),
                  kafkaConfig,
                  serviceLocator.locateAll,
                  topicId.name,
                  eventStreamFactory,
                  partitionKeyStrategy,
                  new ScaladslKafkaSerializer(topicCall.messageSerializer.serializerForRequest),
                  offsetStore,
                  projectionRegistryImpl
                )

              case other =>
                log.warn {
                  s"Unknown topic producer ${other.getClass.getName}. " +
                    s"This will likely result in no events published to topic ${topicId.name} by service ${info.serviceName}."
                }
            }

          case otherTopicImpl =>
            log.warn {
              s"Expected Topic type ${classOf[ScaladslKafkaTopic[_]].getName}, but found incompatible type ${otherTopicImpl.getClass.getName}." +
                s"This will likely result in no events published to topic ${topicId.name} by service ${info.serviceName}."
            }
        }

      case other =>
        log.error {
          s"Cannot plug publisher source for topic ${topicCall.topicId}. " +
            s"Reason was that it was expected a topicHolder of type ${classOf[ScalaMethodTopic[_]]}, " +
            s"but ${other.getClass} was found instead."
        }
    }
  }
}
