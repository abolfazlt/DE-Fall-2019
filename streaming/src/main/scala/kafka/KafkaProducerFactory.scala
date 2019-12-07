package kafka

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

object KafkaProducerFactory {

  val brokers = "localhost:9092"
  val zookeeper = "localhost:2181"

  def createKafkaProducer[K, V](topic: String, keySerializer: String,
                                valueSerializer: String): KafkaProducer[K, V] = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", keySerializer)
    props.put("value.serializer", valueSerializer)
    props.put("request.required.acks", "1")
    //    props.put("zookeeper.connect", zookeeper)

    new KafkaProducer[K, V](props)
  }

}