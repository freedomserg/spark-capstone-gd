package net.learningclub

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

trait Context {

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("SalesProcessor")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)
}
