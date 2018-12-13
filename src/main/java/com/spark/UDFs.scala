package com.spark

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object UDFs{
  def main(args: Array[String]): Unit = {
//    val conf=new SparkConf().setMaster("local").setAppName("UDF");
//    val sc=new SparkContext(conf);
//    val sqlContext=new SQLContext(sc);
//val spark = SparkSession
//  .builder()
//  .appName("SparkSessionZipsExample").config(conf)
//  .enableHiveSupport()
//  .getOrCreate()
//    val names=Array("yarn","Marry","Jack","Tom","Tom","Tom")
//    val namesRDD=sc.parallelize(names,5)
//    val namesRowRDD=namesRDD.map{name=>Row(name)}
//    val structType=StructType(Array(StructField("name",StringType,true)))
//    val  nameDF=sqlContext.createDataFrame(namesRowRDD,structType);
//    nameDF.createOrReplaceTempView("names")
//    sqlContext.udf.register("strLen",(str:String)=>str.length)
//    sqlContext.sql("select name,strLen(name) from names")
//    nameDF.collect().foreach(println)

  }
}
