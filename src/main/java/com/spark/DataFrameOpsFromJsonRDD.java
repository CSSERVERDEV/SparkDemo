package com.spark;

import javafx.scene.input.DataFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.List;

public class DataFrameOpsFromJsonRDD {
    public  static  void  main(String[] args){
        SparkConf sparkConf=new SparkConf().setAppName("DataFrameOpsFromJsonRDD").setMaster("local");
//        SparkContext sparkContext=new SparkContext(sparkConf);
        JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);
        SQLContext sqlContext=new SQLContext(sparkContext);
        List<String> nameList=Arrays.asList("{'name':'Andy1','age':55}","{'name':'Andy2','age':30}","{'name':'Andy3','age':30}","{'name':'Andy4','age':19}");
        List<String> scoreList=Arrays.asList("{'name':'Andy1','score':100}","{'name':'Andy2','score':99}");

        JavaRDD<String> nameRDD=sparkContext.parallelize(nameList);
        JavaRDD<String> scoreRDD=sparkContext.parallelize(scoreList);
        Dataset<Row> nameDF=sqlContext.read().json(nameRDD);
        Dataset<Row> scoreDF=sqlContext.read().json(scoreRDD);
//        nameDF.select(nameDF.col("name").alias("ttName")).show();
//        nameDF.select(nameDF.col("age").plus(100).alias("plugAge")).show();
//        nameDF.join(scoreDF,nameDF.col("name").$eq$eq$eq(scoreDF.col("name")).and(scoreDF.col("score").$eq$eq$eq("99")),"inner")
//                .select(nameDF.col("name"),nameDF.col("age"),scoreDF.col("score")).show();
        nameDF.join(scoreDF,nameDF.col("name").equalTo(scoreDF.col("name")).and(scoreDF.col("score").equalTo("99")),"inner")
                .select(nameDF.col("name"),nameDF.col("age"),scoreDF.col("score")).show();
//        nameDF.join(scoreDF,nameDF.col("name").$eq$eq$eq(scoreDF.col("name"))).select(nameDF.col("name"),nameDF.col("age"),scoreDF.col("score")).show();
//        nameDF.filter(nameDF.col("age").gt(30)).show();


    }
}
