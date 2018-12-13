import com.spark.Person;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

public class SparkHello {
    public static void main(String[] args) throws Exception {
//        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String accessKeyId = "";
        String secretKey = "";

        SparkSession spark = SparkSession.builder()
                .appName("Filter S3 files")
                .config("fs.s3n.awsAccessKeyId", accessKeyId).config("fs.s3n.awsSecretAccessKey", secretKey)
                .master("local").getOrCreate();
        List<Person> personpList = new ArrayList<Person>();
        Person person1 = new Person();
        person1.setName("Andy");
        person1.setAge(32);
        Person person2 = new Person();
        person2.setName("Justin");
        person2.setAge(19);
        personpList.add(person1);
        personpList.add(person2);
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                personpList,
                personEncoder
        );
        javaBeanDS.show();


        //数据导入方式
        Dataset<Row> df = spark.read().json("..\\sparkTestData\\people.json");
        //查看表
        df.show();
        //查看表结构
        df.printSchema();
        //查看某一列 类似于MySQL： select name from people
        df.select("name").show();
        //查看多列并作计算 类似于MySQL: select name ,age+1 from people
        df.select(df.col("name"), df.col("age").plus(1)).show();
        //设置过滤条件 类似于MySQL:select * from people where age>21
        df.filter(df.col("age").gt(21)).show();
        //做聚合操作 类似于MySQL:select age,count(*) from people group by age
        df.groupBy("age").count().show();
        //上述多个条件进行组合 select ta.age,count(*) from (select name,age+1 as "age" from people) as ta where ta.age>21 group by ta.age
        df.select(df.col("name"), df.col("age").plus(1).alias("age")).filter(df.col("age").gt(21)).groupBy("age").count().show();

        //直接使用spark SQL进行查询
        //先注册为临时表
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
    }
}
