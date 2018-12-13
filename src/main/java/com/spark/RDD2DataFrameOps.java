package com.spark;

import javafx.scene.input.DataFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class RDD2DataFrameOps {
    public static  void  main(String[] args){
        SparkConf sparkConf=new SparkConf().setAppName("RDD2DataFrameOps").setMaster("local");
        JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);
        SQLContext sqlContext=new SQLContext(sparkContext);
        JavaRDD<String> lins=sparkContext.textFile("..\\sparkTestData\\people.txt");

        JavaRDD<Person>  personRdd=lins.map(new Function<String, Person>() {
            @Override
            public Person call(String s) throws Exception {
                String[] split=s.split(",");
                Person p=new Person();
                p.setId(Integer.parseInt(split[0].trim()));
                p.setName(split[1].trim());
                p.setAge(Integer.parseInt(split[2].trim()));

                return p;
            }
        });
       Dataset<Row> resultDataFormat= sqlContext.createDataFrame(personRdd,Person.class);
        resultDataFormat.show();
        resultDataFormat.registerTempTable("personTable");
        Dataset<Row> resultDataFormat2=sqlContext.sql("select * from personTable where age>2");
        resultDataFormat2.show();
        JavaRDD<Row> resultRDD=resultDataFormat2.javaRDD();
        JavaRDD<Person> result = resultRDD.map(new Function<Row, Person>() {

            @Override
            public Person call(Row row) throws Exception {
                Person p=new Person();
                p.setId(row.getInt(1));
                p.setName(row.getString(2));
                p.setAge(row.getInt(0));
                return p;
            }
        });
        List<Person> personList=result.collect();
        for (Person obj:personList){
            System.out.println(obj.toString());
        }
    }

}
