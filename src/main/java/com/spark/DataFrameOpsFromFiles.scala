package com.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameOpsFromFiles{
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("DataFrameOpsFromFile").setMaster("local");
    val sc=new SparkContext(conf);
    val sqlContext=new SQLContext(sc);
//    val  df=sqlContext.read.format("json").load("..\\sparkTestData\\people.json");
    val df =sqlContext.read.json("..\\sparkTestData\\people.json")
    df.registerTempTable("people");
    sqlContext.sql("select * from people where age >20").show();
  }
}
