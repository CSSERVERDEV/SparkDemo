package com.spark

import org.apache.spark.sql.SparkSession

object MastCputTask{
  def main (args: Array[String]): Unit = {
    val sparkSession= SparkSession.builder().appName("MastCputTask").master("local").getOrCreate()
    val  sc=sparkSession.sparkContext
    val rdd=sc.textFile("C:\\Users\\zengsong\\IdeaProjects\\sparkTestData\\people.txt")
    rdd.count()
    sc.stop()
    sparkSession.close()
  }
}
