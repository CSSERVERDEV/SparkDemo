package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

public class HiveContextTest {
    public  static  void  main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("DataFrameOpsFromJsonRDD").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        HiveContext hiveContext=new HiveContext(sparkContext);
    }
}
