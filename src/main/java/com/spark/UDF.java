package com.spark;

import com.SparkSessionUtil;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * 一对一
 */
public class UDF {
    public static void main(String[] args){
        SparkSession session=SparkSession.builder().appName("UDF").master("local").getOrCreate();
        session.sql("");
        SQLContext sqlContext=session.sqlContext();
        sqlContext.sql("");
        session.read().text("");
        session.read().jdbc("","",SparkSessionUtil.getConnectionProperties());
        Dataset rdd=session.read().csv();
        //         ./spark-submit --master spark://Savle1:7077 --executor -memory 512M --class org.apache.spark.examples.SparkPi ../lib/spark-examples-1.6.0-hadoop2.6.0.jar 30  #往spark添加一个任务




























    }
}
