# sparkstream-rabbitmq 


```
import com.gizwits.rabbitmq.RabbitMQUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

object RabbitMQReceiver {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("RabbitMQReceiver")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint(".")
    val rabbitMQHosts = "localhost"



    val rbs = RabbitMQUtils.createStreamFromRoutingKeys(ssc,
      Option("topqueue"),
      rabbitMQHosts,
      5672,
      "/",
      "guest",
      "guest",
      "topic_logs_exchange",
      List("log.*"),
      Option("topic"),
      false,
      false,
      1000,
      2,
      StorageLevel.MEMORY_AND_DISK_SER_2)



    rbs.print()
    ssc.start()

    ssc.awaitTermination()


  }
}
```