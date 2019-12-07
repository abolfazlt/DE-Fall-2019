package spark


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

object SparkFactory extends Serializable {

  def createSparkSession(appName: String, duration: Long): (SparkSession, SparkContext, StreamingContext) = {

    val sparkConf = new SparkConf()
      .setMaster("spark://localhost:7077")
      .setJars(Seq(
        "/mnt/storage/SourceCode/spark/target/scala-2.11/spark-assembly-0.1.jar"
      ))
      //      .set("spark.streaming.backpressure.enabled",Configs.get("spark.streaming.backpressure.enabled"))
      //      .set("spark.streaming.backpressure.initialRate",Configs.get("spark.streaming.backpressure.initialRate"))
      //      .set("spark.streaming.kafka.maxRatePerPartition",Configs.get("spark.streaming.kafka.maxRatePerPartition"))
      //      .set("spark.streaming.kafka.consumer.cache.enabled",Configs.get("spark.streaming.kafka.consumer.cache.enabled"))
      //      .set("spark.serializer",Configs.get("spark.serializer"))
      //      .set("spark.driver.extraClassPath",Configs.get("spark.driver.extraClassPath"))
      //      .set("spark.driver.maxResultSize",Configs.get("spark.driver.maxResultSize"))
      //      .set("spark.driver.memory","500m")
      //      .set("spark.driver.memoryOverhead",Configs.get("spark.driver.memoryOverhead"))
      //      .set("spark.executor.memory","500m")
      //      .set("spark.executor.memoryOverhead","100m")
//      .set("spark.executor.cores", "8")
      //      .set("spark.cores.max","1")
      //      .set("spark.executor.extraJavaOptions",Configs.get("spark.executor.extraJavaOptions"))
//      .set("spark.executor.instances", "1")

    val spark = SparkSession
      .builder()
      .config(sparkConf).appName(appName)
//      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(duration))

    (spark, sc, ssc)
  }

  def startStream(ssc: StreamingContext): Unit = {
    ssc.start()
    //    new Thread() {
    //      override def run() {
    //      }
    //    }.start()
    //    sys.ShutdownHookThread {
    //      ssc.stop(stopSparkContext = true, stopGracefully = true)
    //    }
    ssc.awaitTermination()
    ssc.stop()
  }
}