import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ScalaKafkaStreaming {
  def main(args: Array[String]): Unit = {
    // offset保存路径
    val checkpointPath = "./kafka-direct"
//
//    val conf = new SparkConf()
//      .setAppName("ScalaKafkaStream")
//    //.setMaster("local[2]")
//
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

     // Create a local StreamingContext with two working thread and batch interval of 1 second.
     // The master requires 2 cores to prevent a starvation scenario.

     val confi = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
     val ssc=new StreamingContext(confi,Seconds(20))
    ssc.checkpoint(checkpointPath)

    val bootstrapServers = "localhost:9092"
    val groupId = "kafka-test-group"
    val topicName = "KAFKA_TEST"
    val maxPoll = 20000

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val kafkaTopicDS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("test"), kafkaParams))

    kafkaTopicDS.map(_.value)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .transform(data => {
        val sortData = data.sortBy(_._2, false)
        sortData
      })
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
