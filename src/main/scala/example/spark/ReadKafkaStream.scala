package example.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * @author Geek
 * @date 2021-06-24 19:50:14
 *
 * https://spark.apache.org/docs/2.4.3/structured-streaming-kafka-integration.html
 *
 */
object ReadKafkaStream {
  val APP_NAME = "ReadKafkaStream"

  def main(args: Array[String]): Unit = {
    val inputTopic = "web_stream"
    val kafkaBrokers = "localhost:9092"

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(APP_NAME)
      .getOrCreate()

    val df1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("startingOffsets", "earliest")
      //.option("endingOffsets" , "latest") // batch only
      .option("subscribe", inputTopic)
      .option("failOnDataLoss", true) // streaming only
      .load()

    val structType = new StructType()
      .add("product", StringType, true)
      .add("category", StringType, true)
      .add("ts", StringType, true)

    val df2 = df1.selectExpr(
      "cast (key as string)",
      "cast ( value as string) as value",
      "cast ( topic as string)",
      "cast ( partition as int)",
      "cast ( offset as long)",
      "cast ( timestamp as long)",
      "cast ( timestampType as int)"
    )
      .select(
        from_json(col("value"), structType).as("value"),
        col("key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("timestampType")
      )
      .select(
        "*", 
        "value.*"
      )

    df2.writeStream
      .format("console")
      .start()
      .awaitTermination(10 * 1000)

    //df2.show(false) // batch only

    println("df2.isStreaming = " + df2.isStreaming)

  }
  
/*
-------------------------------------------
Batch: 0
-------------------------------------------
+--------------------+-------+----------+---------+------+----------+-------------+-------+--------+----------+
|               value|    key|     topic|partition|offset| timestamp|timestampType|product|category|        ts|
+--------------------+-------+----------+---------+------+----------+-------------+-------+--------+----------+
|[PD0021, Books, 1...|cus_001|web_stream|        2|     0|1624529946|            0| PD0021|   Books|1516978415|
|[PD0022, TV, 1517...|cus_001|web_stream|        2|     1|1624529953|            0| PD0022|      TV|1517978415|
+--------------------+-------+----------+---------+------+----------+-------------+-------+--------+----------+

df2.isStreaming = true
 */
}
