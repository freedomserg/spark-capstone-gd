package net.learningclub

import net.learningclub.Util._

object SalesProcessor extends App with Context {

  import sqlContext.implicits._

  val salesRdd = buildSalesRdd(sc).persist()
  val topCategories = salesRdd
    .map(r => r.split(","))
    .map(r => r(3)) // take category field
    .map(c => (c, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .take(10)

  writeToMySql(
    sc.makeRDD(topCategories).toDF("category", "sales_number"),
    "test",
    "top_categories_spark")


  val topProducts = salesRdd
    .map(r => r.split(","))
    .map(r => (r(3), (r(0), 1))) // category -> (product, 1)
    .combineByKey[List[(String, Int)]](
      (pair: (String, Int)) => List(pair),
      (listPairs: List[(String, Int)], current: (String, Int)) => {
        val pairsMap = listPairs.toMap
        val actualMap = pairsMap.get(current._1) match {
          case None => pairsMap + current
          case Some(sales) => pairsMap + (current._1 -> (sales + current._2))
        }
        actualMap.toList
      }
      ,
      (left: List[(String, Int)], right: List[(String, Int)]) => {
        left.foldLeft(right) { case (accum, current) =>
          val accumMap = accum.toMap
          val updatedAccumMap = accumMap.get(current._1) match {
            case None => accumMap + current
            case Some(sales) => accumMap + (current._1 -> (sales + current._2))
          }
          updatedAccumMap.toList
        }
      }
    )
    .mapValues(_.sortBy(_._2).reverse.take(10))
    .flatMap { r =>
      val category = r._1
      r._2.map(nameAndSales => (nameAndSales._1, category, nameAndSales._2))
    }
    .sortBy(_._2) // just to have the same output as exporting from Hive

  writeToMySql(
    topProducts
      .toDF("name", "category", "sales_number"),
    "test",
    "top_products_in_categories_spark")


  val countriesRdd = sc.textFile("hdfs:///tmp/countries")
  val countriesPairRdd = countriesRdd
    .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
    .map(r => r.split(","))
    .map(r => (r(0), List(r(0), r(1), r(3), r(5)))) // 0 - geoname_id, 1 - locale_code, 3 - continent_name, 5 - country_name


  val ipsRdd = sc.textFile("hdfs:///tmp/ips")
  val ipsPairRdd = ipsRdd
    .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
    .map(r => r.split(","))
    .map(r => (r(1), List(r(0).split("/")(0)))) // 1 - geoname_id, 0 - network

  val countriesAndIPsPairRddByGeonameID =
    countriesPairRdd
      .join(ipsPairRdd) // Countries is on the left side since this table is less
      .mapValues(p => p._2 ::: p._1) // network, geoname_id, locale_code, continent_name, country_name

  val countriesAndIPsPairRddByIP = countriesAndIPsPairRddByGeonameID
    .values
    .filter(r => r(4) != "") // filter out records without country name
    .map(r => (r(0), List(r(0), r(4)))) // 0 - network, 4 - country_name

  val salesCountries = salesRdd
    .map(r => r.split(","))
    .map(r => (r(4), List(r(0), r(1), r(3)))) // 4 - IP, 0 - name, 1 - price, 3 - category
    .join(countriesAndIPsPairRddByIP) // Sales is on the left side since this table is less
    .mapValues(p => p._1 ::: p._2) // merge columns: 0 - name, 1 - price, 2 - category, 3 - network, 4 - country_name
    .values

  val topCountriesByMoney = salesCountries
    .map(r => (r(4), r(1).toDouble)) // take country_name and price
    .reduceByKey(_ + _)
    .sortBy(_._2, false)
    .take(10)

  writeToMySql(
    sc.makeRDD(topCountriesByMoney).toDF("name", "spending"),
    "test",
    "top_countries_by_spending_spark")
}
