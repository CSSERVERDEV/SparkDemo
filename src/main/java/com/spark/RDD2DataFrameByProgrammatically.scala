package com.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object RDD2DataFrameByProgrammatically {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD2DataFrameByReflectionScala").setMaster("local");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    val people=sc.textFile("..\\sparkTestData\\score.txt");
    val scjemaString="clazz:String score:Integer"
    import sqlContext.implicits._
    val rowRDD=people.map(_.split("\t")).map(p=>Row(p(0),p(1).toInt))
    val schema=StructType{scjemaString.split(" ").map(fieldName=>
      StructField(
        fieldName.split(":")(0),
        if(fieldName.split(":")(1).equals("string"))StringType else IntegerType,true))}
    val peopleDataFrame=sqlContext.createDataFrame(rowRDD,schema)
    peopleDataFrame.registerTempTable("clazzscore")
    val result=sqlContext.sql("select clazz,score from clazzscore")
    result.map(r => "clazz:"+r.getAs[String]("clazz")+"score:"+r.getAs[Integer]("score")).collect().foreach(println)
  }

}
