package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SaveModelTesy {
    public static  void  main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("RDD2DataFrameOps").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);
        Dataset<Row> jsonData = sqlContext.read().format("").json("");
        jsonData.write().mode("Append").save("");
    }
}
