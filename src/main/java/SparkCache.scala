import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * 持久化，减少网络传输
  */
object SparkCache{
  def main(args: Array[String]): Unit = {
//    val conf=new SparkConf().setAppName("CoalesceOperators").setMaster("local");
//    val sc=new SparkContext(conf);
//    val lineRDD=sc.textFile("..\\sparkTestData\\people.txt")
//    val cacheRDD=lineRDD.persist(StorageLevel.DISK_ONLY)
//    val startTime=System.currentTimeMillis();
//    val  cacheCount=cacheRDD.count();
//    val  endTime=System.currentTimeMillis()
//    println(endTime-startTime)
//    val  mapOut=cacheRDD.map(lineRDD=> lineRDD.split("{"));
//    println(mapOut)iostat -x 10
    var sparkSession=SparkSession.builder().appName("SparkCache").master("local").getOrCreate();
    val sc=sparkSession.sparkContext
//    sc.setCheckpointDir("hdfs://192.219.164.26:9000/checkpoint_test")
    sc.setCheckpointDir("hdfs://192.219.164.26:9000/")
//    val lineRDD=sc.textFile("C:\\Users\\zengsong\\IdeaProjects\\sparkTestData\\sample.txt")
val lineRDD=sc.textFile("blogtest.html")
    val cacheRDD=lineRDD.persist(StorageLevel.DISK_ONLY)
    cacheRDD.checkpoint()
//    val statTime=System.currentTimeMillis()
val count=cacheRDD.count()
//    val endTime=System.currentTimeMillis()
//    println((endTime-statTime)+"count:"+count)
//    while (true){}
  }
}
