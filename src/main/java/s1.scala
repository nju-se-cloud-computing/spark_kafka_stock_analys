import java.util.Properties

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

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
    //val sc=new SparkContext(confi).setLogLevel("Error")
    //val ssc=StreamingContext
     val ssc=new StreamingContext(confi,Seconds(30))
    ssc.sparkContext.setLogLevel("Error")

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
    val updateFunc=(values:Seq[Int],state:Option[Int])=>{
      val currentCount=values.foldLeft(0)(_+_)
      val previousCount=state.getOrElse(0)
      Some(currentCount+previousCount)
    }


    val updateFunc2=(values:Seq[Double],state:Option[Double])=>{
      val currentCount=values.foldLeft(0.0)(_+_)
      val previousCount=state.getOrElse(0.0)
      Some(currentCount+previousCount)
    }
    //
    val beforemulti=(x:Array[String])=>{
      val label=x(0)+" "+x(3);
      val extent=x(1).toDouble * x(2).toDouble;
      //val circulation=x(1).toDouble
      (label,x(1).toDouble,extent);
    }
    val getOneAndTwo=(x:Tuple3[String,Double,Double])=>{
      (x._1,x._2)
    }
    val getOneAndThree=(x:Tuple3[String,Double,Double])=>{
      (x._1,x._3)
    }
    val lines=kafkaTopicDS
    val words=lines.map(_.value)
    //flatMap(_.split(" "))
    val numerator=kafkaTopicDS.map(_.value).map(_.split(" ")).map(beforemulti);
    val lowersum=numerator.map(getOneAndTwo).updateStateByKey(updateFunc2);
    val upsum  =numerator.map(getOneAndThree).updateStateByKey(updateFunc2)
    val tmp=lowersum.join(upsum).map(x=>{
      val numerator=x._2._2
      val denominator=x._2._1
      val stri=x._1.split(" ")
      var ans=0.0
      //if(denominator!=0){ans=numerator/denominator}
      (stri(0),stri(1),numerator)
    })
    upsum.print()
    lowersum.print()



    val uri="mongodb+srv://Frank:123456789shi@cluster0.2hsme.gcp.mongodb.net/Homework.test3?retryWrites=true&w=majority"
    val options = scala.collection.mutable.Map(

      //大多数情况下从primary的replica set读，当其不可用时，从其secondary members读
      "readPreference.name" -> "primaryPreferred",

      "spark.mongodb.input.uri" -> uri,
      "spark.mongodb.output.uri" -> uri)

    val writeConfig=WriteConfig(options)
    val documents=tmp.map(temp=>{

    var docMap=  new Document("module",temp._1);
      docMap.append("date",temp._2)
    docMap.append("numerator",temp._3.toString)
      //print(docMap.entrySet())
    docMap
    })
    documents.foreachRDD(rdd=>{MongoSpark.save(rdd, writeConfig)})



    //val pairs = words.map { word => (word, 1)}
    //pairs.map(word=>(word,"!!!!!~!!!!")).print()
    //val wordCounts = pairs.reduceByKey(_ + _)
   // val wordCounts=pairs.updateStateByKey[Int](updateFunc)
   // wordCounts.print()
    //      .flatMap(_.split(" "))
//      .map(x => (x, 1L))
//      .reduceByKey(_ + _)
//      .transform(data => {
//        val sortData = data.sortBy(_._2, false)
//        sortData
//      })
//      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
