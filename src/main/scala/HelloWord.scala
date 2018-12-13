import Array._
object HelloWord {
  def main (args: Array[String]): Unit = {
    println("Hello Word!")
    var num=0
    num=2
    num=6
    println(num)
    var age="dfdf"
    age="aa"
    println(age)

    var myList1 = Array(1.9, 2.9, 3.4, 3.5)
    var myList2 = Array(8.9, 7.9, 0.4, 1.5)

    var myList3 =  concat( myList1, myList2)

    // Print all the array elements
    for ( x <- myList3 ) {
      println( x )
    }
  }
}