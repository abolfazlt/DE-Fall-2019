package kafka

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.dstream.InputDStream
import scala.reflect.ClassTag
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaConsumerFactory extends Serializable {

  val brokers = "localhost:9092"
  val zookeeper = "localhost:2181"
  val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers,
    "zookeeper.connect" -> zookeeper,
    "producer.type" -> "async",
    "auto.offset.reset" -> "earliest",
    "max.poll.records" -> "2000",
    "request.required.acks" -> "0",
    "batch.num.messages" -> "500",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream")

  def createKafkaMessageStream[K: ClassTag, V: ClassTag](
                                                          topicsSet: Array[String], ssc: StreamingContext): InputDStream[ConsumerRecord[K, V]] = {
    val message = KafkaUtils.createDirectStream[K, V](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[K, V](topicsSet, kafkaParams))

    message
  }

}