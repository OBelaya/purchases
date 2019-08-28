import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/*
  To run via spark-submit on yarn:

  export JAVA_HOME="/usr/java/jdk1.8.0_162/"
  spark-submit --class LabSpark --master yarn --executor-memory 1G  labSpark-assembly-0.1.jar


  sqoop export --connect jdbc:mysql://34.67.117.142/spark --username root --password 1234567 --table result51hive --hcatalog-database obila —-hcatalog-table result51

  */

object LabSpark {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("lab")

    val sc = new SparkContext(conf)
    val csv = sc.textFile("hdfs:///flume/events/*/*/*/*.csv")

    // create purchase rdd
    val purchaseRdd = csv.map(row => {
      val arr = row.split(",");
      Purchase(arr(0), arr(1).toDouble, arr(2), arr(3), arr(4))
    })

    //result 5.1
    val result51 = purchaseRdd.map(p => (p.category, 1))
      .aggregateByKey(0)( (acc, v) => acc + v, (acc1, acc2) => acc1 + acc2).sortBy(_._2, ascending = false)

    println("\n\n\nSolution 5.1:")
    result51.take(10).foreach(println(_))

    //result 5.2
    val result52 = purchaseRdd.map(p => ((p.category, p.name), 1))
      .aggregateByKey(0)((acc, v) => acc + v, (acc1, acc2) => acc1 + acc2)
      .groupBy(_._1._1).mapValues(l => l.toList.sortWith(_._2 > _._2).map(e => (e._1._2, e._2)).take(10))
      .flatMapValues(x => x).map{ case (c, (n, o)) => (c, n, o)}

    println("\n\n\nSolution 5.2:")
    result52.collect().foreach(println(_))


    // create ips rdd
    val csvIpMask = sc.textFile("hdfs:///user/obila/geo_cidr/GeoLite2-Country-Blocks-IPv4.csv")
    val headerIpMask = csvIpMask.first()
    val ipMaskRdd = csvIpMask.filter(_ != headerIpMask)
      .flatMap(row => {
        val arr = row.split(",")
        val geonameId = List(arr(1), arr(2), arr(3)).find(_ != "")
        geonameId.map(id => CountryMask(arr(0), id.toInt, extractMinIpNumber(arr(0)), extractMaxIpNumber(arr(0))))
      }).keyBy(_.geonameId)

    // create countries rdd
    val csvCountries = sc.textFile("hdfs:///user/obila/geo_country/GeoLite2-Country-Locations-en.csv")
    val headerCountries = csvCountries.first()
    val countriesRdd = csvCountries.filter(_ != headerCountries)
      .map(row => {
        val arr = row.split(",")
        Country(arr(0).toInt, arr(5))
      }).keyBy(_.geonameId)

    // ip data joined with countries data
    val geoJoined = ipMaskRdd.join(countriesRdd).sortBy(_._2._1.minIpNum).collect()

    //broadcast joined data
    sc.broadcast(geoJoined)

    //result 6.4
    val result64 = purchaseRdd
      .map(p => (getCountryByIp(p.ip, geoJoined), p))
      .aggregateByKey(0d)((acc, v) => acc + v.price, (acc1, acc2) => acc1 + acc2)
      .filter(_._1 != null)
      .sortBy(_._2, ascending = false)

    println("\n\n\nSolution 6.4:")
    result64.take(10).foreach(println)

    //export results to DB
    val sparkSession = SparkSession.builder().getOrCreate()
    exportToDb("result51", sparkSession.createDataFrame(result51).limit(10))
    exportToDb("result62", sparkSession.createDataFrame(result52))
    exportToDb("result64", sparkSession.createDataFrame(result64).limit(10))

  }

  private def getCountryByIp(ip: String, geoData: Array[(Int, (CountryMask, Country))]) = {
    val ipNum = ipToNum(ip)
    findCountryWithBinarySearch(0, geoData.length - 1, ipNum, geoData)
  }

  private def findCountryWithBinarySearch(begin: Int, end: Int, ipNum: Long, geoData: Array[(Int, (CountryMask, Country))]): String = {
    if (begin > end) return null
    val pos = begin + (end - begin + 1) / 2
    val value = geoData(pos)
    if (ipNum >= value._2._1.minIpNum && ipNum <= value._2._1.maxIpNum) value._2._2.country
    else if (ipNum < value._2._1.minIpNum) findCountryWithBinarySearch(begin, pos - 1, ipNum, geoData)
    else findCountryWithBinarySearch(pos + 1, end, ipNum, geoData)
  }

  private def extractMinIpNumber(network: String) = {
    val subnetUtils = new SubnetUtils(network)
    subnetUtils.setInclusiveHostCount(true)
    val networkInfo = subnetUtils.getInfo
    ipToNum(networkInfo.getLowAddress)
  }

  private def extractMaxIpNumber(network: String) = {
    val subnetUtils = new SubnetUtils(network)
    subnetUtils.setInclusiveHostCount(true)
    val networkInfo = subnetUtils.getInfo
    ipToNum(networkInfo.getHighAddress)
  }

  private def ipToNum(ip: String) = {
    val arr = ip.split("/")(0).split("\\.")
    (arr(0).toLong << 24) + (arr(1).toLong << 16) + (arr(2).toLong << 8) + arr(3).toLong
  }

  private def exportToDb(tableName: String, df: DataFrame): Unit = {
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "1234567")

    val url = "jdbc:mysql://34.67.117.142:3306/spark"
    df.write.mode("append").jdbc(url, tableName, prop)
  }

  case class Purchase(name: String, price: Double, timestamp: String, category: String, ip: String)

  case class CountryMask(network: String, geonameId: Int, minIpNum: Long, maxIpNum: Long)

  case class Country(geonameId: Int, country: String)

}