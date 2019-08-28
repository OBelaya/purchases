import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.Socket
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import net.andreinc.mockneat.MockNeat

import scala.math.BigDecimal.RoundingMode
import scala.util.Random

object EventProducer {
  val PORT = 9999
  val EVENTS_COUNT = 3500
  val IP_RANGE = 256
  val DATE_RANGE = 7
  val GAUSS_PRICE_MEAN = 40000
  val GAUSS_PRICE_DELTA = 20000
  val DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss"

  def main(args: Array[String]): Unit = {
    val socket = new Socket("localhost", PORT)
    val out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
    for(_ <- 1 to EVENTS_COUNT) {
      val str = generateRandomEvent() + System.lineSeparator()
      out.append(str)
      out.flush()
    }
    socket.shutdownOutput()
  }

  def generateRandomEvent(): String = {
    val rand = new Random()

    //name
    val productNames = Array("mazda cx-5", "suzuki jimny", "toyota corolla", "bmw x6", "tesla model x", "mercedes gle",
    "mini cooper s", "vw golf", "nissan juke", "smart for two", "renault zoe", "audi q8", "dodge challenger", "ford mustang")
    val productName = productNames(rand.nextInt(productNames.length))

    //price
    val productPrice = BigDecimal.valueOf(rand.nextGaussian() * GAUSS_PRICE_DELTA + GAUSS_PRICE_MEAN).setScale(2, RoundingMode.HALF_UP).doubleValue

    //datetime
    val beginDateTime = LocalDateTime.of(2019, 8, 1, 14, 35, 12)
    val formatter = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN)
    val productDate = beginDateTime.plusDays(rand.nextInt(DATE_RANGE)).format(formatter)

    //category
    val productCategories = Array("new", "used", "testdrive", "exclusive", "brand new", "new with defects", "amg tuned",
      "customized", "brabus tuned", "after crash", "online", "refurbished")
    val productCategory = productCategories(rand.nextInt(productCategories.length))

    //ip
    val ipGenerator = MockNeat.threadLocal()
    val ip = ipGenerator.ipv4s().`val`()

    String.join(",", productName, productPrice.toString, productDate.toString, productCategory, ip)
  }

}

