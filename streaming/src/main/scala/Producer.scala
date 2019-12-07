import kafka.KafkaProducerFactory
import org.apache.kafka.clients.producer.{ProducerRecord}
import scala.io.Source

object Producer extends App {
  val kafkaProducer = KafkaProducerFactory.createKafkaProducer[String, String]("test",
    "org.apache.kafka.common.serialization.StringSerializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProducer.send(new ProducerRecord[String, String]("test", "Big Data"))

  Source.fromFile("/mnt/storage/carriot/data/backup.csv").
    getLines().foreach{
    line =>
      kafkaProducer.send(new ProducerRecord[String, String]("test", line));
      Thread.sleep(100)
    }
  Thread.sleep(1000) // wait for 1000 millisecond
}
