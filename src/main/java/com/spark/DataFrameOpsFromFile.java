package com.spark;

import javafx.scene.input.DataFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataFrameOpsFromFile {
    public static void main(String[] args)throws  Exception{
        SparkConf sparkConf=new SparkConf().setAppName("DataFrameOpsFromFile").setMaster("local");
        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        SQLContext sqlContext=new SQLContext(javaSparkContext);
//        sqlContext.read().format("json").load("..\\sparkTestData\\people.json");
        Dataset<Row> df=sqlContext.read().json("..\\sparkTestData\\people.json");
        df.show();//不能读嵌套的格式
        df.printSchema();
        df.registerTempTable("people");
        df.select("age").show();
        df.select(df.col("name"),df.col("age").plus(10)).show();
        df.filter(df.col("age").gt(20)).show();//大于20
        df.groupBy(df.col("age")).count().show();//统计age分组行数
        Dataset<Row> sql=sqlContext.sql("select * from people where age is not NULL");
        sql.show();
    }
}
