import kafka.KafkaConsumerFactory
import spark.SparkFactory
import org.apache.spark.streaming.{Minutes, Seconds, State, StateSpec}

object TFIDF extends App with Serializable{

  val (spark, sc, ssc) = SparkFactory.createSparkSession("DE_Spark", 10)
  val topicsSet = Array[String]("test")
  ssc.checkpoint("/tmp/")
  val stream = KafkaConsumerFactory.createKafkaMessageStream[String, String](
    topicsSet, ssc)

  stream.map(record=>(record.value)).print( )

  val stateSpec = StateSpec.function(updateState _)
    .numPartitions(10)
    .timeout(Minutes(1))

  val stateStream = stream.
    flatMap(record => record.value().split(",").map(str => (str, 1))).mapWithState(stateSpec)
  stateStream.print()
  val stateStream2 = stream.
    flatMap(record => record.value().split(",").
      map(str => (record.value().split(",")(4) + str, 1))).mapWithState(stateSpec)
  stateStream2.print()
  SparkFactory.startStream(ssc)

  def updateState(key: String, value: Option[Int], state: State[Int]): (String, Int, Int) = {
    val prevValue = state.getOption().getOrElse(0)

    if(state.isTimingOut()) {
      (key, prevValue, -1)
    }
    else {
      val nextValue = prevValue + value.getOrElse(0)

      if(nextValue > 10)
        state.remove
      else
        state.update(nextValue)

      (key, prevValue, nextValue)
    }
  }
}
