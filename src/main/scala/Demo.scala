class ThreadExample extends Thread{
  override def run(){
    for(i<- 0 to 5){
      println(i)
      Thread.sleep(60000)
    }
    println("Thread is running?");
  }
}
class  ThreadExample2 extends Runnable{
  override def run(){
    println("Thread is running...")
  }
}
object Demo {
  def main(args: Array[String]) {
    println ("Apply method : " + apply("Maxsu", "gmail.com"));
    println ("Unapply method : " + unapply("Maxsu@gmail.com"));
    println ("Unapply method : " + unapply("Maxsu Ali"))

    var t = new ThreadExample()
    t.start()
    var threadExample2 = new ThreadExample2()
    var t2=new Thread(threadExample2)
    t2.start()
  }

  // The injection method (optional)
  def apply(user: String, domain: String) = {
    user +"@"+ domain
  }

  // The extraction method (mandatory)
  def unapply(str: String): Option[(String, String)] = {
    val parts = str split "@"

    if (parts.length == 2){
      Some(parts(0), parts(1))
    } else {
      None
    }
  }
}