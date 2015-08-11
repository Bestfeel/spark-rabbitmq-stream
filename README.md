# sparkstream-rabbitmq 

    import com.gizwits.util.RabbitMQUtils
    import org.apache.spark.storage.StorageLevel
    import org.apache.spark.streaming._
	import org.apache.spark.{SparkConf, SparkContext}

	object RabbitMQReceiver {
 	 def main(args: Array[String]) {

	    val sparkConf = new SparkConf().setAppName("RaddbitHelloWorld")
	    val sc = new SparkContext(sparkConf)
	    val ssc = new StreamingContext(sc, Seconds(2))
	    ssc.checkpoint(".")
	    val rabbitMQHosts="localhost"
	    val rbs = RabbitMQUtils.createStreamFromRoutingKeys
		(ssc,Option("topqueue"),
	      rabbitMQHosts,
	      5672,
	      "topic_logs_exchange",
	      List("log.*"),
	      Option("topic"),
	      false,
	      StorageLevel.MEMORY_AND_DISK_SER_2)
	    rbs.print()
	    ssc.start()
 
   	   ssc.awaitTermination()

 

	  }
	}

