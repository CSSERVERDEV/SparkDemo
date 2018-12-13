package com.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RDD2DataFrameByReflectionScala {
  case class Person(name: String, age: Int)
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("RDD2DataFrameByReflectionScala").setMaster("local");
    val sc=new SparkContext(conf);
    val sqlContext=new SQLContext(sc);
    import sqlContext.implicits._
    val people=sc.textFile("..\\sparkTestData\\people.txt").map(_.split(",")).map(p => Person(p(1),p(2).trim.toInt)).toDF();
    people.registerTempTable("people")
    val selectorResult=sqlContext.sql("select name,age from people where age>=6 and age <19 ");
    selectorResult.map(t => "Name:"+t(0)).collect().foreach(println)
    selectorResult.map(r => "Name:"+r.getAs[String]("name")).collect().foreach(println)
  }
}
