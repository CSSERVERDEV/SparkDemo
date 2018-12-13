package com.spark.scala

import java.io.FileNotFoundException

import scala.util.control.Breaks

/**
  * scala语言基础操作
  */
object ScalaBase{
  def main (args: Array[String]): Unit = {

//    val age1,age2,age3=0 //1行多个变量申明，val是不可变变量
//    var age=0
//     age=1  //重新赋值
//
//    0.to(5)   //To方法   结果为一个数据集合Range<0,1,2,3,4,5>
//
//    printf("%s is the future's Framwork ","Spark")  //输出
//    readLine            //读入
//    readLine("xxx")
//    readInt
    //if语句
    val age=19
    var buffered = 0
    if (age>18) {
      "adult" }else{ "child"
    }
    val result=if (age>10){
      "adult"
      buffered=10
      buffered  //最后一行的值是整个代码块的返回值
    }

    //循环语句
    val element=10
    for (i <- 0 to element if (i%3==0)) printf("%d",i) //添加判断语句，是元素的判断
    for (i <- 0 to element) printf("%d",i)
       //需要导入 scala.util.control.Breaks
    for (i <- 0 to 10){
      if (i==4){Breaks}
      println(i)
    }


    //函数定义
    val n=10
    def f1:Any={        //Any类型是因为返回的类型无法确定
      for (i<- 0 to 20){
        if (i==n) return 10
        println(i)
      }
    }
//    def f3(param1:String,param2:Int=20)=param1+param2
//    f3("Spark")                    //函数调用
//    f3(param2=100,param1="Scala")  //可以改变参数顺序
////    def sum(number:Int,number1: Int,number2: Int,number3: Int,number4: Int)={       //求和，元素个数任意
//    def sum(number:Int*)={       //求和，元素个数任意
//      sum(1,2,3,4,6)
//      sum(1 to 100:_*)   //-*把每个元素都提取出来
//      var result=0;
//      for (element<-number) result +=element
//      result
//    }
//    def morming(content:String) { }
//    def morming1(content:String):Unit=" " //过程，没有返回值


//    lazy val content=fromFile("test.txt")   //导入 scala.io.Source._

//    try{
//      val content=fromFile("...").mkString
//    }catch {
//      case _:FileNotFoundException => println("can not find")
//    }finally {
//      println("Bye!!")
//    }
    //数组
//    val arr=new  Array[Int](5)    //新建数组包含5个元素
//    arr(3)=4                     //改变第3个元素
//    import scala.collection.mutable.ArrayBuffer
//    val arrBuffer=ArrayBuffer[Int]()  //大小可以变的数组
//    arrBuffer+=10                     //添加元素
//    arrBuffer++=Array(1,4,3,4)
//    arrBuffer.trimEnd(3)              //截掉后面3个元素
//    arrBuffer.insert(5,2,3)           //在第5个元素后加入2，3
//    arrBuffer.remove(6,3)             //从第6个元素开始移除3个
//    val arr2=arrBuffer.toArray
//    arr2.toBuffer
//    for (elem <- arr2) println(elem)
//    for (i<-0 until(arr2.length,2)) println(arr(i))  //间隔输出
//    for (i<- (0 until arr2.length).reverse) println(arr(i))
//    arr2.sum  //逆序输出
////    scala.util.Sorting.quickSort<arrBuffer>
//    val arr3=for (i<-arr if (i%2==0)) yield  i*i
//    arr.filter(_%3==0).map(i=>i*i)
//    //map
//    val person=Map("spark"->6,"hadoop"->11)
//    val persons=scala.collection.mutable.Map("spark"->6,"hadoop"->11)
//    persons += ("Filnk"->5)    //添加
//    persons -= "Flink"         //删除
//    for {
//      (key,value)<-person
//    } println(key+":"+value)
//    val person=scala.collection.immutable.SortedMap("spark"->6,"hadoop"->11)        //对Map排序
//    val tuple("spark",6,99.9)   //元组 包含各种类型
//    tuple._1                    //spark
    //移除一个数组中第一个负数后的所有负数
    val arr=Array(1,34,-6,23,-1,24,-12,-23,23,45,-4,-34,-136,19)
    var bool,num,tol=0
    for (i <- 0 until(arr.length,1)){
      if (bool==0 && arr(i)< 0) bool=1
      else if (bool==1){
        if (arr(i) < 0){
          num+=1
          tol+=1
        }else arr(i-num)=arr(i)
      }
    }
    val arr2=arr.toBuffer
    arr2.remove(arr.length-tol,tol)
    arr2.mkString(",")
  }
}
