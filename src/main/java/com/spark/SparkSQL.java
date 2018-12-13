package com.spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkSQL {
    private static final Boolean IS_DEBUG=true;
    private static final String JDBC_URL="jdbc:mysql://127.0.0.1:3306/spark";
    public static  void  main(String[] args){
        SparkSession sparkSession = null;
        String accessKeyId = "";
        String secretKey = "";

        if(IS_DEBUG) {
            sparkSession = SparkSession.builder()
                    .appName("Filter S3 files")
                    .config("fs.s3n.awsAccessKeyId", accessKeyId).config("fs.s3n.awsSecretAccessKey", secretKey)
                    .master("local").getOrCreate();
        }else {
            sparkSession = SparkSession.builder()
                    .appName("Filter S3 files")
                    .config("fs.s3n.awsAccessKeyId", accessKeyId).config("fs.s3n.awsSecretAccessKey", secretKey)
                    .getOrCreate();
        }
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3n.awsAccessKeyId", accessKeyId);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secretKey);
        sparkSession.sparkContext().hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2");


        Dataset<Row> resultData=null;
        //读取源数据
        Properties jdbcConnectionProperties = new Properties();
        jdbcConnectionProperties.put("user","root");
        jdbcConnectionProperties.put("password","root");
        jdbcConnectionProperties.put("driver","com.mysql.jdbc.Driver");

        Dataset<Row> oldDeviceRegister  = sparkSession.read().jdbc(JDBC_URL,"order",jdbcConnectionProperties).select("*");
        oldDeviceRegister.createOrReplaceTempView("OrderInfo");
//        oldDeviceRegister.filter(oldDeviceRegister.col("variable").gt("statement_truncate_len"));
//        Dataset<Row> oldDeviceRegisters  = sparkSession.read().jdbc(JDBC_URL,"sys_detail",jdbcConnectionProperties).select("*");
        oldDeviceRegister.show(100000);
//        oldDeviceRegister.createOrReplaceTempView("SysConfig");
        Dataset<Row> countData = sparkSession.sql("select count(1) from OrderInfo group by code");
        countData.show();
        //数据入库 spark
//        resultData.write().mode("append").jdbc(JDBC_URL,"sys_config", jdbcConnectionProperties);
        //mode用于指定将DataFrame保存到数据源的预期行为。
//        if (args == null || args.length < 3) {
//            throw new IllegalArgumentException("parameter error");
//        }

//        String input = args[0];//test-tmpdir
//        String output = args[1];//test-logtype
//        String start = args[2];//2017-04-26
//        String end = "";
//        if (StringUtils.isEmpty(input)
//                || StringUtils.isEmpty(output)
//                || StringUtils.isEmpty(start)
//                ) {
//            throw new IllegalArgumentException("parameter error");
//        }
        sparkSession.close();
    }
}
