package com;

import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Created by ti on 2018/4/16.
 */
public class SparkSessionUtil {
    private static final boolean IS_DEBUG = false;
    private static final String JDBC_URL = "jdbc:mysql://127.0.0.1:3306/sprak";
    private static SparkSession sparkSession = null;
    private static Properties jdbcConnectionProperties = null;
    private SparkSessionUtil(){

    }

    static{
        initSparkSession();
        initConnectionProperties();
    }

    public static SparkSession getSparkSession() {
        return sparkSession;
    }

    public static Properties getConnectionProperties() {
        return jdbcConnectionProperties;
    }

    public static String getJdbcUrl() {
        return JDBC_URL;
    }

    private static void initConnectionProperties() {
        jdbcConnectionProperties = new Properties();
        jdbcConnectionProperties.put("user","root");
        jdbcConnectionProperties.put("password","123456");
        jdbcConnectionProperties.put("driver","com.mysql.jdbc.Driver");
    }

    private static void initSparkSession(){


        if(IS_DEBUG) {
            String accessKeyId = "";
            String secretKey = "";

            sparkSession = SparkSession.builder()
                    .appName("Filter S3 files")
                    .config("fs.s3n.awsAccessKeyId", accessKeyId).config("fs.s3n.awsSecretAccessKey", secretKey)
                    .master("local").getOrCreate();

            sparkSession.sparkContext().hadoopConfiguration().set("fs.s3n.awsAccessKeyId", accessKeyId);
            sparkSession.sparkContext().hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secretKey);
        }else {
            sparkSession = SparkSession.builder()
                    .appName("Filter S3 files")
                    .getOrCreate();
        }

        sparkSession.sparkContext().hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2");
    }
}