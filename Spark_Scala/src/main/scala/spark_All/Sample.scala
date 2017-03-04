package spark_All
import org.apache.spark._
import org.apache.spark.SparkConf
object Sample {
  def main(args:Array[String]){
   //Local 
    val a=Array(10,20,30,40)
    a.foreach(println)
   //Conf
    val conf=new SparkConf().setMaster("local[2]").setAppName("Sample");
    //SparkContext
    val sc=new SparkContext(conf);
    println("spark context created ")
    val b=sc.parallelize(a);
    //Transformation on RDD
    val c=b.map(x=>if(x>30 )"H" else "L");
    System.out.println("Transformation done");
    c.collect.foreach(println);
    //reading data from file
    val data=sc.textFile("src/data");
    val res=data.flatMap(x=>x.split(" "))
   res.collect.foreach(println) 
   res.saveAsTextFile("target/op");
    
  }
}