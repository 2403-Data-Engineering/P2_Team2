import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MySparkJob")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("Alice", 30),
      ("Bob", 25),
      ("Carol", 35)
    )

    val df = data.toDF("name", "age")

    df.show()

    spark.stop()
  }
}