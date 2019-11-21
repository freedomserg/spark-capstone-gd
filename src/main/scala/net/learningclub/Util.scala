package net.learningclub

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}

object Util {

  def buildSalesRdd(sc: SparkContext): RDD[String] = {
    val salesRdd1 = sc.textFile("hdfs:///tmp/flume/events/19/08/31")
    val salesRdd2 = sc.textFile("hdfs:///tmp/flume/events/19/09/01")
    val salesRdd3 = sc.textFile("hdfs:///tmp/flume/events/19/09/02")
    val salesRdd4 = sc.textFile("hdfs:///tmp/flume/events/19/09/03")
    val salesRdd5 = sc.textFile("hdfs:///tmp/flume/events/19/09/04")
    val salesRdd6 = sc.textFile("hdfs:///tmp/flume/events/19/09/05")
    val salesRdd7 = sc.textFile("hdfs:///tmp/flume/events/19/09/06")

    salesRdd1
      .union(salesRdd2)
      .union(salesRdd3)
      .union(salesRdd4)
      .union(salesRdd5)
      .union(salesRdd6)
      .union(salesRdd7)
  }

  def createMySqlConnectionPropsTo(db: String): Properties = {
    Class.forName("com.mysql.jdbc.Driver")
    val connectionProps = new Properties()
    connectionProps.put("user", "root")
    connectionProps.put("password", "cloudera")
    connectionProps
  }

  def writeToMySql(df: DataFrame, db: String, table: String): Unit = {
    val props = createMySqlConnectionPropsTo(db)
    df
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(s"jdbc:mysql://quickstart.cloudera:3306/$db", table, props)
  }

}
