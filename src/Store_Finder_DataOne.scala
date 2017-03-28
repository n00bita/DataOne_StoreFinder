
/* -------------------------------------------------------------------------------------------
 * Name : Store_Finder_DataOne.scala
 * Author : n00bita
 * Date : 2017-03-29  
 * Description : Given Input in <data-file>.csv (comma-Delimited) format with various stores having different products available 
 * as different combo-packs.Finding the Set of combination of combo-packs/individual items such that we buy all the required 
 * products in our List.A variant of Set-Cover Algorithm which has been solved by Greedy Approach here.
 * 
 *------------------------------------------------------------------------------------------*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

object Store_Finder_DataOne {

  def findStoreMinimum(SubsetList: Array[Set[String]], priceList: Array[Double], prodBuySet: scala.collection.mutable.Set[String]):Double = {
    var cartBuy = scala.collection.mutable.Set[String]()
    var costArray = new Array[Double](priceList.length)
    var totalCost=0.0
    println("=====================Entered CartBuy===========================")
    while (!(prodBuySet subsetOf cartBuy)) {
        var index = 0
        for (i <- 0 to SubsetList.length -1) {
            val costSet = priceList(i)
            val SubsetListRow = SubsetList(i)
            if (!(SubsetListRow subsetOf cartBuy)) {
                val diff = SubsetListRow diff cartBuy
                costArray(i) = costSet / diff.size}
            else
                costArray(i) = 999.00
                println("SubsetListRow:"+SubsetListRow)
            }
        println("cartBuy:"+cartBuy) 
        costArray.foreach(x=>print(x+" "))
        println
        val minimum = costArray.min
        while (costArray(index) != minimum) 
            index +=1
        val bestSet = SubsetList(index)
        cartBuy = cartBuy union bestSet
        val addPrice =priceList(index)
        totalCost+=addPrice
    }
    println("=====================Exited CartBuy===========================")
    totalCost
  }
  

  case class Products(STORE_ID: Int, PRODUCT_PRICE: Double, PRODUCT_LIST: String)
  
  case class Output(STORE_ID: Int,MINIMUM_PRICE : Double)

  def globalMinimum(feasibleShops : Array[Int],concernedData: RDD[Products],prodBuySet
                    :scala.collection.mutable.Set[String]):scala.collection.mutable.Map[Int, Double]={
    val minLocalMap = collection.mutable.Map[Int, Double]()
    for(STORE<- feasibleShops){
        val priceList = concernedData.filter(x=>x.STORE_ID==STORE).map(x => x.PRODUCT_PRICE).collect
        val SubsetList = concernedData.filter(x=>x.STORE_ID==STORE)
           .map(x => ((for (item <- x.PRODUCT_LIST split ",") yield item.trim).toSet)).collect
    //Find minimum cost local to each Shop
        minLocalMap += (STORE -> findStoreMinimum(SubsetList,priceList,prodBuySet))       
    }
    //Find minimum cost globally
    minLocalMap
    }
  
  
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("DataOne_StoreFinder").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    
    val data = sc.textFile(args(0)) //val data = sc.textFile("data.csv")
    val productRDD = data.map(_.split(",", 3)).map(fields => Products(fields(0).toInt, fields(1).toDouble, fields(2).toString))

    var prodBuySet = scala.collection.mutable.Set[String]() //val prodBuySet=Set("scissor","bath_towel")
    for (i <- 1 to args.length - 1) {
      prodBuySet += args(i)
    }
    
    val feasibleShops = productRDD
       .flatMap(row => for (item <- row.PRODUCT_LIST split ",") yield (row.STORE_ID, item))
       .groupByKey()
       .map(x => (x._1.toInt, (for (x <- x._2) yield x.trim).toSet))
       .filter(prodBuySet subsetOf _._2).map(_._1).collect
       
//Use Spark to filter if he have massive amount of Data.
    val concernedData = productRDD.filter(x => feasibleShops contains x.STORE_ID)
       .filter { x => ((for (item <- x.PRODUCT_LIST split ",") yield item.trim)
           .toSet intersect prodBuySet isEmpty) == false }
    
    val minLocalMap=globalMinimum(feasibleShops,concernedData,prodBuySet)
    println("Minimum Price : "+minLocalMap.max._2+", Store Id : "+minLocalMap.max._1)

//Using DataFrames to manipulate our Data
    val productDF = productRDD.toDF
    productDF.createOrReplaceTempView("STORE")
    val productDF_Select = sqlContext.sql("""
      |SELECT * FROM store
      |""".stripMargin)
    productDF_Select.show(false)
    
    val concernedDataDF = concernedData.toDF
    concernedDataDF.createOrReplaceTempView("PROD_TEMP")
    val concernedDataDF_Select = sqlContext.sql(""" 
      |SELECT * FROM prod_temp
      |""".stripMargin)
    concernedDataDF_Select.show(false)
    
  // Convert Map to Seq so it can passed to parallelize  
    val OutputDF = sc
    .parallelize(minLocalMap.toSeq).map(fields => Output(fields._1.toInt,fields._2.toDouble)).toDF
    OutputDF.write.parquet("stores.parquet")
    OutputDF.createOrReplaceTempView("MIN_PRICES")
    OutputDF.select("STORE_ID","MINIMUM_PRICE").show()
    val OutputDF_Select = sqlContext.sql(""" 
      |SELECT * FROM MIN_PRICES WHERE MINIMUM_PRICE =(SELECT min(MINIMUM_PRICE) FROM MIN_PRICES)
      |""".stripMargin)
    OutputDF_Select.show(false)    

    

  }
}
