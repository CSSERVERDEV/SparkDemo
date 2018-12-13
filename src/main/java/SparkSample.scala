
import org.apache.spark.{SparkConf, SparkContext}

object SparkSample{
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("CoalesceOperators").setMaster("local");
    val sc=new SparkContext(conf);
    val lineRDD=sc.textFile("..\\sparkTestData\\sample.txt")
//    val cacheRDD=lineRDD.persist(StorageLevel.DISK_ONLY)
//    val startTime=System.currentTimeMillis();
//    val  cacheCount=cacheRDD.count();
//    val  endTime=System.currentTimeMillis()
//    println(endTime-startTime)
    val  mapOut=lineRDD.map(lineRDD=> lineRDD.split("\\s+"));
    println(mapOut.first().toString)
  }
}
