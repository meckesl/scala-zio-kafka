package com.lms

import zio.{Has, RManaged, ZIO, ZLayer, console}
import zio.clock.Clock
import zio.blocking.Blocking
import zio.json._
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}
import zio.kafka.consumer._
import zio.kafka.serde.Serde
import zio.stream.{ZSink, ZStream}

import scala.util.{Failure, Success}

object FPKafkaConsumer extends App {

  val consumerSettings =
    ConsumerSettings(List("localhost:9092"))
      .withGroupId("stocks-consumer")

  // RManaged = ressource that cannot fail
  // Blocking provides access to a thread pool
  //

  val managedConsumer: RManaged[Clock with Blocking, Consumer] =
    Consumer.make(consumerSettings)

  val consumer: ZLayer[Clock with Blocking, Throwable, Has[Consumer]] =
    ZLayer.fromManaged(managedConsumer)

  val matchesStreams: ZStream[Has[Consumer], Throwable, CommittableRecord[String, String]] =
    Consumer.subscribeAnd(Subscription.topics("updates"))
      .plainStream(Serde.string, Serde.string)

  val matchesStreamsWithPoisonPill: ZStream[Has[Consumer], Throwable, CommittableRecord[String, String]] =
    Consumer.subscribeAnd(Subscription.topics("updates"))
      .plainStream(Serde.string, matchSerde.asTry)
      .map(cr => (cr.value, cr.offset))
      .tap { case (tryMatch, _) =>
        tryMatch match {
          case Success(matchz) => console.putStrLn(s"${matchz.score}")
          case Failure(ex) => console.putStrLn(s"Poison pill ${ex.getMessage}")
        }
      }

  val itaMatchesStreams: SubscribedConsumerFromEnvironment =
    Consumer.subscribeAnd(Subscription.pattern("updates|.*ITA.*".r))

  val partitionedMatchesStreams: SubscribedConsumerFromEnvironment =
    Consumer.subscribeAnd(Subscription.manual("updates", 1))


  case class Player(name: String, score: Int) {
    override def toString: String = s"$name: $score"
  }

  case class Match(players: Array[Player]) {
    def score: String = s"${players(0)} - ${players(1)}"
  }

  object Player {
    implicit val decoder: JsonDecoder[Player] = DeriveJsonDecoder.gen[Player]
    implicit val encoder: JsonEncoder[Player] = DeriveJsonEncoder.gen[Player]
  }

  object Match {
    implicit val decoder: JsonDecoder[Match] = DeriveJsonDecoder.gen[Match]
    implicit val encoder: JsonEncoder[Match] = DeriveJsonEncoder.gen[Match]
  }

  val matchSerde: Serde[String, Match] =
    Serde.string.inmapM { matchAsString =>
      ZIO.fromEither(matchAsString.fromJson[Match].left.map(new RuntimeException(_)))
    } { matchAsObj =>
      ZIO.effect(matchAsObj.toJson)
    }

  matchesStreams
    .map(cr => (cr.value.score, cr.offset))
    .tap { case (score, _) => console.putStrLn(s"| $score |") }
    .map { case (_, offset) => offset }
    .aggregateAsync(Consumer.offsetBatches)
    .run(ZSink.foreach(_.commit))

}
