package com.lms

import zio.{RManaged, ZLayer}
import zio.clock.Clock
import zio.blocking.Blocking
import zio.kafka.consumer._

class KafkaConsumer {

  val consumerSettings =
    ConsumerSettings(List("localhost:9092"))
      .withGroupId("stocks-consumer")

  // RManaged = ressource that cannot fail
  // Blocking provides access to a thread pool
  //

  val managedConsumer: RManaged[Clock with Blocking, Consumer.Service] = Consumer.make(consumerSettings)

  val consumer: ZLayer[Clock with Blocking, Throwable, Consumer] =
    ZLayer.fromManaged(managedConsumer)

}
