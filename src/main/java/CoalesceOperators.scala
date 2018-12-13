import org.apache.spark.{SparkConf, SparkContext}

object CoalesceOperators{
  def main(args: Array[String]): Unit = {
    val sc=new SparkConf().setAppName("CoalesceOperators").setMaster("local")
    val spark=new SparkContext(sc)
    val lineRDD=spark.textFile("..\\sparkTestData\\people.txt")
    val cacheRDD=lineRDD.cache()
    val rDDCount=lineRDD.count()
    println(cacheRDD)
    println("cache rdd count:"+rDDCount)
    println(lineRDD)
    val array={};
    println(array)
  }
}
