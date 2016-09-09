
package com.gizwits.rabbitmq

import com.rabbitmq.client._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import scala.util._


class RabbitMQInputDStream(
                            @transient ssc_ : StreamingContext,
                            rabbitMQQueueName: Option[String],
                            rabbitMQHost: String,
                            rabbitMQPort: Int,
                            exchangeName: Option[String],
                            routingKeys: Seq[String],
                            DirectExchangeType:Option[String],
                            ack:Boolean,
                            storageLevel: StorageLevel
                            ) extends ReceiverInputDStream[String](ssc_) with Logging {

  override def getReceiver(): Receiver[String] = {
    val DefaultRabbitMQPort = 5672

    new RabbitMQReceiver(rabbitMQQueueName,
      Some(rabbitMQHost).getOrElse("localhost"),
      Some(rabbitMQPort).getOrElse(DefaultRabbitMQPort),
      exchangeName,
      routingKeys,
      DirectExchangeType.getOrElse("direct"),
        ack,
      storageLevel)
  }
}


class RabbitMQReceiver(rabbitMQQueueName: Option[String],
                       rabbitMQHost: String,
                       rabbitMQPort: Int,
                       exchangeName: Option[String],
                       routingKeys: Seq[String],
                       DirectExchangeType:String,
                       ack:Boolean,
                       storageLevel: StorageLevel)
  extends Receiver[String](storageLevel) with Logging {

 // val DirectExchangeType: String = "direct"

  def onStart() {
    implicit val akkaSystem = akka.actor.ActorSystem()
    getConnectionAndChannel match {
      case Success((connection: Connection, channel: Channel)) => receive(connection, channel,ack)
      case Failure(f) => log.error("Could not connect"); restart("Could not connect", f)
    }
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive(connection: Connection, channel: Channel,ack:Boolean) {

    val queueName = !routingKeys.isEmpty match {
      case true => {
        channel.exchangeDeclare(exchangeName.get, DirectExchangeType)
       // val queueName = channel.queueDeclare().getQueue()
        channel.queueDeclare(rabbitMQQueueName.get, false, false, false, null)

        for (routingKey: String <- routingKeys) {
          channel.queueBind(rabbitMQQueueName.get, exchangeName.get, routingKey)
        }
        rabbitMQQueueName.get
      }
      case false => {
       // channel.queueDeclare(rabbitMQQueueName.get, false, false, false, new com.gizwits.AkkaActor.util.HashMap(0))
        rabbitMQQueueName.get
      }
    }

    log.info("RabbitMQ Input waiting for messages")
    val consumer: QueueingConsumer = new QueueingConsumer(channel)
   // val ack = false
    channel.basicConsume(queueName, ack, consumer)

    while (!isStopped) {
      val delivery: QueueingConsumer.Delivery = consumer.nextDelivery
      store(new String(delivery.getBody))

      if(!ack){
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }


    }

    log.info("it has been stopped")
    channel.close
    connection.close
    restart("Trying to connect again")
  }

  private def getConnectionAndChannel: Try[(Connection, Channel)] = {
    for {
      connection: Connection <- Try(getConnectionFactory.newConnection())
      channel: Channel <- Try(connection.createChannel)
    } yield {
      (connection, channel)
    }
  }

  private def getConnectionFactory: ConnectionFactory = {
    val factory: ConnectionFactory = new ConnectionFactory
    factory.setHost(rabbitMQHost)
    factory.setPort(rabbitMQPort)
    factory
  }
}
