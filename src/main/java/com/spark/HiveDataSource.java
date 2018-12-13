package com.spark;

import org.apache.spark.sql.*;

public class HiveDataSource {
    public static void main(String[] args){
        SparkSession sparkSession=SparkSession
                .builder()
                .enableHiveSupport()
                .appName("HiveDataSource")
                .master("local")
                .getOrCreate();
        SQLContext hiveContext=sparkSession.sqlContext();
        //删除数据表
        hiveContext.sql("drop table if exists student_infos");
        //创建表
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name string,age int) row format dolimitod fields terminated by  '\t'");
        hiveContext.sql("LOAD DATA LOCAL INPATH 'C:\\Users\\zengsong\\IdeaProjects\\sparkTestData\\people.json' INTO TABLE student_infos");//加载数据

        hiveContext.sql("drop table if exists student_scores");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name string,score int) row format dolimitod fields terminated by '\t'");
        hiveContext.sql("LOAD DATA LOCAL INPATH 'C:\\Users\\zengsong\\IdeaProjects\\sparkTestData\\people.json' INTO TABLE student_scores");//加载数据

        Dataset<Row> goodStudentsDF=hiveContext.sql("select from student_infos si join student_scores ss on si.name=ss.name where si.age>80");

        hiveContext.sql("USE result");
        hiveContext.sql("drop table if exists good_student_infos");
        goodStudentsDF.write().saveAsTable("good_student_infos");
//        Row[] row=hiveContext.table("good_student_infos").collect();
//        for (Row r:row){
//            System.out.println(r);
//        }
        sparkSession.close();
    }
}

