package net.learningclub

import net.learningclub.Util.writeToMySql
import org.apache.spark.sql.functions._

object SalesProcessorDatasets extends App with Context {

  import sqlContext.implicits._

  val salesDS =
    sqlContext.read.text(
      "hdfs:///tmp/flume/events/19/08/31",
      "hdfs:///tmp/flume/events/19/09/01",
      "hdfs:///tmp/flume/events/19/09/02",
      "hdfs:///tmp/flume/events/19/09/03",
      "hdfs:///tmp/flume/events/19/09/04",
      "hdfs:///tmp/flume/events/19/09/05",
      "hdfs:///tmp/flume/events/19/09/06").as[String]
      .map { r =>
        val records = r.split(",")
        Sale(records(0), records(1).toDouble, records(2), records(3), records(4))
      }.persist()

  val topCategories = salesDS
    .groupBy(s => s.category).count()
    .map(r => SalesPerCategory(r._1, r._2))
    .toDF().sort($"sales_number".desc)
    .map { r =>
      val category = r.getAs[String]("category")
      val sales_number = r.getAs[Long]("sales_number")
      (category, sales_number.toInt)
    }
    .take(10)

  writeToMySql(
    sc.makeRDD(topCategories).toDF("category", "sales_number"),
    "test",
    "top_categories_spark")


  //  val byCategory = Window.partitionBy($"category").orderBy("sales_number")
  val topProducts =
    salesDS.toDF()
      .groupBy($"category", $"name")
      .agg(count($"name").as("sales_number"))
      //    .withColumn("rank", rank over byCategory)
      // org.apache.spark.sql.AnalysisException: Could not resolve window function 'rank'. Note that, using window functions currently requires a HiveContext;
      .orderBy($"category", $"sales_number".desc)
      .map { r =>
        val category = r.getAs[String]("category")
        val product = r.getAs[String]("name")
        val salesNumber = r.getAs[Long]("sales_number")
        SalesPerProductInCategory(product, category, salesNumber.toInt)
      }
      .map(s => (s.category, s))
      .combineByKey(
        (s: SalesPerProductInCategory) => List(s),
        (sales: List[SalesPerProductInCategory], current: SalesPerProductInCategory) => current :: sales,
        (left: List[SalesPerProductInCategory], right: List[SalesPerProductInCategory]) => left ::: right
      )
      .mapValues(_.sortBy(_.sales_number).reverse.take(10))
      .values
      .flatMap(r => r.map(s => (s.name, s.category, s.sales_number)))
      .sortBy(_._2)

  writeToMySql(
    topProducts
      .toDF("name", "category", "sales_number"),
    "test",
    "top_products_in_categories_spark")

  val countriesDF = sqlContext.read.text("hdfs:///tmp/countries").as[String]
    .map { r =>
      val record = r.split(",") //0 - geoname_id, 1 - locale_code, 3 - continent_name, 5 - country_name
      CountryData(record(0), record(1), record(3), record(5))
    }
    .toDF()

  val ipsDF = sqlContext.read.text("hdfs:///tmp/ips").as[String]
    .map { r =>
      val record = r.split(",") // 1 - geoname_id, 0 - network
      IpAddressData(record(1), record(0).split("/")(0))
    }
    .toDF()

  val ipCountryDF = countriesDF.join(ipsDF, "geonameId")
    // retain network and country
    .drop($"geonameId")
    .drop($"localeCode")
    .drop($"continent")
    .where($"country" !== "")


  val salesDF = salesDS.toDF()
  val countriesByMoney = salesDF.join(ipCountryDF, salesDF.col("ipAddress") === ipCountryDF.col("network"))
    // name, price, timestampDate, category, ipAddress, network, country
    // retain country and price
    .drop($"name")
    .drop($"timestampDate")
    .drop($"category")
    .drop($"ipAddress")
    .drop($"network")
    .groupBy($"country")
    .agg(sum($"price").as("price"))
    .sort($"price".desc)
    .take(10)

  writeToMySql(
    sc
      .makeRDD(countriesByMoney)
      .map(r => (r.getAs[String]("country"), r.getAs[Double]("price"))).toDF("name", "spending"),
    "test",
    "top_countries_by_spending_spark")

}

case class Sale(name: String,
                price: Double,
                timestampDate: String,
                category: String,
                ipAddress: String)

case class SalesPerCategory(
                             category: String,
                             sales_number: Long
                           )

case class SalesPerProductInCategory(
                                      name: String,
                                      category: String,
                                      sales_number: Long
                                    )

case class CountryData(
                        geonameId: String,
                        localeCode: String,
                        continent: String,
                        country: String
                      )

case class IpAddressData(
                          geonameId: String,
                          network: String
                        )
