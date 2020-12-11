package statistic.warehouse

import org.apache.spark.sql.functions.{avg, max, min, round}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset, Row}

object Statistic {

  private val warehouseSchema = StructType(
    List(
      StructField("positionId", LongType),
      StructField("warehouse", StringType),
      StructField("product", StringType),
      StructField("eventTime", LongType)
    )
  )

  private val amountSchema = StructType(
    List(
      StructField("positionId", LongType),
      StructField("amount", DecimalType(10, 2)),
      StructField("eventTime", LongType)
    )
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Warehouse statistics")
      .master("local[*]")
      .config("spark.driver.bindAddress", "localhost")
      .getOrCreate()

    val warehousesDF = spark.read
      .format("csv")
      .option("header", "true")
      .schema(warehouseSchema)
      .load("src/main/warehouse-data/warehouses.csv")
      .toDF()

    val amountOfWarehouseDF = spark.read
      .format("csv")
      .option("header", "true")
      .schema(amountSchema)
      .load("src/main/warehouse-data/amount_of_warehouses.csv")
      .toDF()

    getCurrentAmountOfEachPosition(warehousesDF, amountOfWarehouseDF).show

    getStatistics(warehousesDF, amountOfWarehouseDF).show

  }

  private def getCurrentAmountOfEachPosition(
    warehousesDF: DataFrame,
    amountOfWarehouseDF: DataFrame
  ): Dataset[Row] = {
    val maxAmountTimeDf = amountOfWarehouseDF
      .groupBy("positionId")
      .agg(max("eventTime").as("eventTime"))

    val buffDf =
      maxAmountTimeDf.join(amountOfWarehouseDF, Seq("positionId", "eventTime"))

    warehousesDF
      .join(buffDf, Seq("positionId"), "inner")
      .select("positionId", "warehouse", "product", "amount")
      .orderBy("positionId")
  }

  private def getStatistics(warehousesDF: DataFrame,
                            amountOfWarehouseDF: DataFrame): Dataset[Row] = {

    val calculatedStatistics = amountOfWarehouseDF
      .groupBy("positionId")
      .agg(
        max("amount").as("Max Amount"),
        min("amount").as("Min Amount"),
        round(avg("amount"), 2).as("Average Amount")
      )

    warehousesDF
      .join(calculatedStatistics, Seq("positionId"))
      .select(
        "positionId",
        "warehouse",
        "product",
        "Max Amount",
        "Min Amount",
        "Average Amount"
      )
      .orderBy("positionId")
  }

}
