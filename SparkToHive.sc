package hive.jobs

import org.apache.spark.sql.SparkSession 

object FileToHive extends App {
    val spark = SparkSession.builder().master("yarn") 
    .appName("Spark To Hive job")
    .config("spark.yarn.jars", "hdfs://192.168.31.14:8020/user/srinivas/jars/*.jar")
    .enableHiveSupport()
    .getOrCreate() 
    /* we can either do enableHiveSupport or copy paste the hivesite.xml on to the resources directory */
    
     
   
    val productsDf = spark.read
    .option("header", "true")
    .csv("/user/srinivas/datasets/products/products-data.csv")
    
    val ordersDf = spark.read
    .option("header", "true")
    .csv("/user/srinivas/datasets/orders/orders-data.csv")
    
    val customersDf = spark.read
    .option("header", "true")
    .csv("/user/srinivas/datasets/customers/customers-data.csv")
    
  val resDf = 
    
    productsDf.write.saveAsTable("spark_to_hive.productsDB")
    
}
