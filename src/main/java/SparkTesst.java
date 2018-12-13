import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTesst {
//    public static void main(String[] args){
//        String accessKeyId = "";
//        String secretKey = "";
//
//        SparkSession spark = SparkSession.builder()
//                .appName("Filter S3 files")
//                .config("fs.s3n.awsAccessKeyId", accessKeyId).config("fs.s3n.awsSecretAccessKey", secretKey)
//                .master("local").getOrCreate();
//        //在Spark 2.0中，window API内置也支持time windows！Spark SQL中的time windows和Spark Streaming中的time windows非常类似。
//        Dataset<Row> stocksDF = spark.read().option("header","true").
//                option("inferSchema","true").
//                csv("..\\sparkTestData\\device.csv").cache();
//
////        stocksDF.show();
//        Dataset<Row> stocks201606 = stocksDF.filter("product_id=70")
////                .filter("year(operate_time)==2015").
////                filter("month(operate_time)==12")
//                ;
////        stocks201606.show(1000,false);//显示100行并不截断长字符串
//        //第二个参数指定了窗口的持续时间(duration)，它的单位可以是seconds、minutes、hours、days或者weeks。
//        Dataset<Row> tumblingWindowDS = stocks201606.groupBy(window(stocks201606.col("operate_time"),"1 week")).
//                agg(avg("product_id").as("weekly_average"));
//        tumblingWindowDS.show(100,false);
//        tumblingWindowDS.sort("window.start").
//                select("window.start","window.end","weekly_average").
//                show(false);
//        //和滑动时间(slide duration)设置成一样来创建带有开始时间的tumbling window。代码如下：
//        Dataset<Row>  windowWithStartTime = stocks201606.
//                groupBy(window(stocks201606.col("operate_time"),"1 week","1 week", "136 hour")).
//                agg(avg("product_id").as("weekly_average"));
//        //6 days参数就是开始时间的偏移量；前两个参数分别代表窗口时间和滑动时间，我们打印出这个窗口的内容：
//        windowWithStartTime.sort("window.start").
//                select("window.start","window.end","weekly_average").
//                show(false);
//    }
public static void main(String[] args) throws InterruptedException {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster("local[2]");
    sparkConf.setAppName("test-for-spark-ui");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    //知识，哪怕是知识的幻影，也会成为你的铠甲，保护你不被愚昧反噬。
//    JavaPairRDD<String,Integer> counts = sc.textFile("C:\\Users\\zengsong\\IdeaProjects\\sparkTestData\\people.txt")
//            .flatMap(line -> Arrays.asList(line.split(" ")))
//            .mapToPair(s -> new Tuple2<String,Integer>(s,1))
//            .reduceByKey((x,y) -> x+y);

//    counts.cache();
//    List<Tuple2<String,Integer>> result = counts.collect();
//    for(Tuple2<String,Integer> t2 : result){
//        System.out.println(t2._1+" : "+t2._2);
//    }
//    sc.stop();
}
}
