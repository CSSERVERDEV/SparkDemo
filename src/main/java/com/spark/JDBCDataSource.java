package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCDataSource {
    public  static  void  main(String[] args) throws SQLException {
        SparkConf sparkConf=new SparkConf().setAppName("DataFrameOpsFromJsonRDD").setMaster("local");
//        SparkContext sparkContext=new SparkContext(sparkConf);
        JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);
        SQLContext sqlContext=new SQLContext(sparkContext);
        //两张表加载为dataFrame
//        Map<String,String> options=new HashMap<String,String>();
//        options.put("url","jdbc:mysql://127.0.0.1:3306/spark");
//        options.put("dbtable","order");
//        options.put("driver","com.mysql.jdbc.Driver");
//        options.put("user","root");
//        options.put("password","root");
//        Dataset<Row> rowDatasetReader=sqlContext.read().format("jdbc").options(options).load();
//        options.put("dbtable","order_log");
//        Dataset<Row> rowDatasetReaderTab2=sqlContext.read().format("jdbc").options(options).load();


        DataFrameReader reader=sqlContext.read().format("jdbc");
        reader.option("url","jdbc:mysql://127.0.0.1:3306/spark");
        reader.option("dbtable","order");
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user","root");
        reader.option("password","root");
        Dataset<Row> couponDF=reader.load();
        reader.option("dbtable","order_log");
        Dataset<Row> couponReleaseLogDF=reader.load();

        //将两个dataset转换为JavaPaiPairRDD,执行join操作
        couponDF.registerTempTable("Order");
        couponReleaseLogDF.registerTempTable("OrderLog");
        String sql="select Order.id,amount,account from Order join OrderLog on (OrderLog.order_id=Order.id) where Order.status=2 ";
        Dataset<Row> sql2=sqlContext.sql(sql);
        sql2.show();

        JavaPairRDD<String, Tuple2<Integer, Integer>> couponRDD = couponDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0),Integer.valueOf(String.valueOf(row.get(1).toString())));
            }
        }).join(couponReleaseLogDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0),Integer.valueOf(String.valueOf(row.get(1).toString())));
            }
        }));
        //转换为JavaRDD
        JavaRDD<Row> couponRowRDD=couponRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                return RowFactory.create(stringTuple2Tuple2._1,stringTuple2Tuple2._2._1,stringTuple2Tuple2._2._2);
            }
        });
        JavaRDD<Row> filterCouponRowRDD=couponRowRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if(row.getInt(2)==2){
                    return true;
                }
                return false;
            }
        });
        //继续转换为DataFrame
        List<StructField> structFieldList=new ArrayList<StructField>();
        structFieldList.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFieldList.add(DataTypes.createStructField("amount",DataTypes.DoubleType,true));
        structFieldList.add(DataTypes.createStructField("account",DataTypes.StringType,true));
        StructType structType=DataTypes.createStructType(structFieldList);

//        Dataset<Row> studentData=sqlContext.createDataFrame(filterCouponRowRDD,structType);
        //遍历数据
//        Row[] rows=studentData.collect();
//        for (Row row:rows){
//            System.out.println(row);
//        }
            sql2.javaRDD().foreach(new VoidFunction<Row>(){
                @Override
                public void call(Row row) throws Exception {
//                String insertSQL="insert into e_coupon values ("+"'"+row.get(0)+"',"+"'"+row.get(1)+"',"+"'"+row.get(2)+"')";
                    String insertSQL=" insert into ops_user (`username`, `password`) values(" + "'" + String.valueOf(row.get(1)) + "'," + "'" + String.valueOf(row.get(2)) + "'),";
                    java.sql.Connection conn = null;
                    java.sql.Statement stmt = null;
                    try {
                        Class.forName("com.mysql.jdbc.Driver");

                        conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/sprak", "root", "root");
                        stmt = conn.createStatement();
                        stmt.executeUpdate(insertSQL);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (stmt != null) {
                            stmt.close();
                        }
                        if (conn != null) {
                            conn.close();
                        }
                    }
                }
            });


        sparkContext.close();
    }
}
