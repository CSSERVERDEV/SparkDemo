import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CoalesceOperator {
    public static void  main(String[] args){
        SparkConf sparkConf=new SparkConf().setAppName("CoalesceOperator").setMaster("local");//spark模式
        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        javaSparkContext.setCheckpointDir("C:\\Users\\zengsong\\IdeaProjects\\sparkTestData");
        List<String> staffList=Arrays.asList("zengsong1","zengsong2","zengsong3","zengsong4","zengsong5","zengsong6","zengsong7","zengsong8","zengsong9","zengsong10",
                "zengsong11","zengsong12","zengsong13","zengsong14","zengsong15","zengsong16","zengsong17","zengsong18","zengsong19","zengsong20","zengsong21","zengsong22","zengsong23","zengsong24");
        JavaRDD<String> staffRDD=javaSparkContext.parallelize(staffList,6);
        staffRDD.checkpoint();
        JavaRDD<String> staffRDD2=staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                List<String> list=new ArrayList<String>();
                while (stringIterator.hasNext()){
                    String staff=stringIterator.next();
                    list.add("部门:["+integer+"]"+staff);
                }
                return list.iterator();
            }
        },true);
        for (String staffInfo:staffRDD2.collect()){
            System.out.println(staffInfo);
        }
        JavaRDD<String> staffRDD3=staffRDD2.coalesce(4,false);
//        JavaRDD<String> staffRDD3=staffRDD2.repartition(12);
        JavaRDD<String> staffRDD4=staffRDD3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                List<String> list=new ArrayList<String>();
                while (stringIterator.hasNext()){
                    String staff=stringIterator.next();
                    list.add("部门:["+integer+"]"+staff);
                }
                return list.iterator();
            }
        },true);
        for (String staffInfo:staffRDD4.collect()){
            System.out.println(staffInfo);
        }
//        List<Tuple2<String,String>> studentList=Arrays.asList(new Tuple2<String,String>("1","xuruyun"),new Tuple2<String,String>("2","wangfei"),new Tuple2<String,String>("3","lixin"));
//        List<Tuple2<String,String>> scoreList=Arrays.asList(new Tuple2<String,String>("1","100"),new Tuple2<String,String>("2","90"),new Tuple2<String,String>("3","80"),new Tuple2<String,String>("1","100")
//                ,new Tuple2<String,String>("2","60"),new Tuple2<String,String>("3","50"));
//        JavaPairRDD<String,String> stdents=javaSparkContext.parallelizePairs(studentList);
//        JavaPairRDD<String,String> scores=javaSparkContext.parallelizePairs(scoreList);
//        JavaPairRDD<String,String> studentJavaPairRDD=stdents.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<Object>>() {
//            @Override
//            public Iterator<Object> call(Integer integer, Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
//                List<Tuple2<String, String>> list=new ArrayList<Tuple2<String, String>>();
//                while (tuple2Iterator.hasNext()){
//                    Tuple2<String, String> staff=tuple2Iterator.next();
//                    list.add(staff);
//                    staff;
//                }
//                return list.iterator();
//            }
//        },true);
    }
}
