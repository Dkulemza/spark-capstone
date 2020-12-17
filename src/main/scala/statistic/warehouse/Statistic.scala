package statistic.warehouse

import org.apache.spark.sql.functions.{avg, max, min, round}
import org.apache.spark.sql.{SparkSession, Dataset, Row}

object Statistic {

  val spark = SparkSession
    .builder()
    .appName("Warehouse statistics")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    getCurrentAmountOfEachPosition(
      "src/main/resources/warehouse-data/amount_of_warehouses.csv",
      "src/main/resources/warehouse-data/warehouses.csv"
    ).show

    getStatistics(
      "src/main/resources/warehouse-data/amount_of_warehouses.csv",
      "src/main/resources/warehouse-data/warehouses.csv"
    ).show

  }

  private def getCurrentAmountOfEachPosition(
    amountPath: String,
    warehousePath: String
  ): Dataset[Row] = {
    val warehousesDF = DataReader.readWarehouseData(warehousePath)
    val amountOfWarehouseDF = DataReader.readAmountOfWarehouse(amountPath)

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

  private def getStatistics(amountPath: String,
                            warehousePath: String): Dataset[Row] = {

    val warehousesDF = DataReader.readWarehouseData(warehousePath)
    val amountOfWarehouseDF = DataReader.readAmountOfWarehouse(amountPath)

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
