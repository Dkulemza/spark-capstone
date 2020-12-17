package statistic.warehouse

import java.io.{BufferedWriter, FileWriter}
import java.util.concurrent.ThreadLocalRandom

import au.com.bytecode.opencsv.CSVWriter

import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal

import scala.collection.JavaConverters._

object DataGenerator {
  private val W: String = "W-"
  private val P: String = "P-"

  private val AmountOfRecords: Int = 500

  private var warehouseFields =
    Array("positionId", "warehouse", "product", "eventTime")
  private var amountFields = Array("positionId", "amount", "eventTime")

  private var warehouses: ListBuffer[Array[String]] =
    new ListBuffer[Array[String]]
  private var amountOfWarehouses: ListBuffer[Array[String]] =
    new ListBuffer[Array[String]]

  private val random: ThreadLocalRandom = ThreadLocalRandom.current()

  def main(args: Array[String]): Unit = {

    warehouses += warehouseFields
    amountOfWarehouses += amountFields

    for (id <- 1 to AmountOfRecords) {
      val timestamp = System.currentTimeMillis() - random.nextInt(
        48 * 60 * 60 * 1000
      )
      warehouses += warehouseGenerator(id, timestamp)
      amountsGenerator(id)
    }
    writeToCSV(warehouses, "src/main/resources/warehouse-data/warehouses.csv")
    writeToCSV(
      amountOfWarehouses,
      "src/main/resources/warehouse-data/amount_of_warehouses.csv"
    )
  }

  private def warehouseGenerator(id: Int, timestamp: Long): Array[String] = {
    val warehouse = W + id
    val product = P + random.nextInt(1, 200).toString
    Array(id.toString, warehouse.toString, product, timestamp.toString)
  }

  private def amountsGenerator(id: Int): Unit = {
    val amount = random.nextInt(1, 4)
    for (_ <- 1 to amount) {
      val timestamp = System.currentTimeMillis() - random.nextInt(
        48 * 60 * 60 * 1000
      )
      amountOfWarehouses += Array(
        id.toString,
        generateBigDecimal().toString(),
        timestamp.toString
      )
    }

  }

  private def generateBigDecimal(): BigDecimal = {
    val amount = random.nextDouble(1, 1000)
    BigDecimal(amount).setScale(2, BigDecimal.RoundingMode.HALF_UP)
  }

  private def writeToCSV(data: ListBuffer[Array[String]],
                         path: String): Unit = {
    val output = new BufferedWriter(new FileWriter(path))
    val csv = new CSVWriter(output)

    try {
      csv.writeAll(data.asJava)
    } finally {
      csv.close()
      output.close()
    }
  }

}
