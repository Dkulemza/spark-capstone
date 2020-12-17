package statistic.warehouse

import org.apache.spark.sql.types.{DecimalType, LongType, StringType, StructField, StructType}
import statistic.warehouse.Statistic.spark

object DataReader {

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


  def readAmountOfWarehouse(path: String) = {
    spark.read
      .format("csv")
      .option("header", "true")
      .schema(amountSchema)
      .load(path)
      .toDF()
  }

  def readWarehouseData(path: String) = {
    spark.read
      .format("csv")
      .option("header", "true")
      .schema(warehouseSchema)
      .load(path)
      .toDF()
  }

}
