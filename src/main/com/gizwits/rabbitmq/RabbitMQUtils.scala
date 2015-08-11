package com.gizwits.rabbitmq

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import scala.reflect.ClassTag

object RabbitMQUtils {

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   * @param ssc                StreamingContext object
   * @param rabbitMQHost       Url of remote RabbitMQ server
   * @param rabbitMQPort       Port of remote RabbitMQ server
   * @param rabbitMQQueueName  Queue to subscribe to
   * @param storageLevel       RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createStreamFromAQueue(ssc: StreamingContext,
                   rabbitMQHost: String,
                   rabbitMQPort: Int,
                   rabbitMQQueueName: String, ack:Boolean,
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                    ): ReceiverInputDStream[String] = {
    new RabbitMQInputDStream(
      ssc,
      Some(rabbitMQQueueName),
      rabbitMQHost,
      rabbitMQPort,
      None,
      Seq(),
    None,
    ack,
      storageLevel)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   * @param jssc               JavaStreamingContext object
   * @param rabbitMQHost       Url of remote RabbitMQ server
   * @param rabbitMQPort       Port of remote RabbitMQ server
   * @param rabbitMQQueueName  Queue to subscribe to
   * @param storageLevel       RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createJavaStreamFromAQueue(jssc: JavaStreamingContext,
                   rabbitMQHost: String,
                   rabbitMQPort: Int,
                   rabbitMQQueueName: String,
                                 ack:Boolean,
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStreamFromAQueue(jssc.ssc, rabbitMQHost, rabbitMQPort, rabbitMQQueueName,ack)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   * @param ssc              StreamingContext object
   * @param rabbitMQHost     Url of remote RabbitMQ server
   * @param rabbitMQPort     Port of remote RabbitMQ server
   * @param exchangeName     Exchange name to subscribe to
   * @param routingKeys      Routing keys to subscribe to
   * @param storageLevel     RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createStreamFromRoutingKeys(ssc: StreamingContext,
                                  rabbitMQQueueName: Option[String],
                   rabbitMQHost: String,
                   rabbitMQPort: Int,
                   exchangeName: String,
                   routingKeys: Seq[String],
                                  DirectExchangeType:Option[String],
                                  ack:Boolean,
                   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                    ): ReceiverInputDStream[String] = {
    new RabbitMQInputDStream(
      ssc,
      rabbitMQQueueName,
      rabbitMQHost,
      rabbitMQPort,
      Some(exchangeName),
      routingKeys,
      DirectExchangeType,
      ack,
      storageLevel)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   * @param jssc             JavaStreamingContext object
   * @param rabbitMQHost     Url of remote RabbitMQ server
   * @param rabbitMQPort     Port of remote RabbitMQ server
   * @param exchangeName     Exchange name to subscribe to
   * @param routingKeys      Routing keys to subscribe to
   * @param storageLevel     RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createJavaStreamFromRoutingKeys(jssc: JavaStreamingContext,
                                      rabbitMQQueueName:String,
                                  rabbitMQHost: String,
                                  rabbitMQPort: Int,
                                  exchangeName: String,
                                  routingKeys: java.util.List[String],
                                      DirectExchangeType:String,
                                      ack:Boolean,
                                  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                                   ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStreamFromRoutingKeys(jssc.ssc,Option(rabbitMQQueueName), rabbitMQHost, rabbitMQPort, exchangeName, scala.collection.JavaConversions
      .asScalaBuffer(routingKeys),Option(DirectExchangeType),ack, storageLevel)
  }
}
