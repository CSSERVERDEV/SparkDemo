package com.spark

import java.util
import java.util.{HashMap, Map}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object  JDBCDataSources{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameOpsFromJsonRDD").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val options: util.Map[String, String] = new util.HashMap[String, String]
    options.put("url", "jdbc:mysql://127.0.0.1:3306/sys")
    options.put("user", "root")
    options.put("password", "root")
    options.put("dbtable", "student_info")
    val studentInfoDF=sqlContext.read.format("jdbc").options(options).load()
    options.put("dbtable", "student_score")
    val studentScoreDF=sqlContext.read.format("jdbc").options(options).load()
//    val rdd1=studentInfoDF.map(x=>(x.getString(0),x.getInt(1)))
//    val rdd2=studentScoreDF.map(x=>(x.getString(0),x.getInt(1)))
//    val studentRDD=rdd1.join(rdd2);
//    val studentRowRDD=studentRDD.map(x=>())

  }
}
