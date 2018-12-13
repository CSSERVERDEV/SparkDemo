package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

public class ParquestLocalData {
    public  static  void main(String[] args){
        SparkConf config=new SparkConf().setAppName("ParquestLocalData").setMaster("local");
        SparkContext sparkContext=new SparkContext(config);
        HiveContext hiveContext=new HiveContext(sparkContext);
        hiveContext.jdbc("","");
    }
}
