package com.csu.fa17.cs535.tp

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object HFTMain {

  case class sch(userID: String, itemID: String, rating: String, time: String, wordCount: String, reviewText: String)

    def main(args: Array[String]) {


      val fileLocation = "/home/bbkstha/CSU/I/BigData/Project/data/SnapData/Review/Uncleaned/"
      val fileName = "Electronics"
      val saveDataforHFT = "/home/bbkstha/CSU/I/BigData/Project/data/SnapData/Review/CleanforHFT"

      val spark = SparkSession
        .builder()
        .appName("ALS_Train")
        .master("local")
        .getOrCreate()
      import spark.implicits._

      val rawData = spark.read.json(fileLocation+fileName+".json").toDF();
      //rawData.show(5)

//      val schemaString = "userID itemID rating time wordCount reviewText"
//      // Generate the schema based on the string of schema
//      val fields = schemaString.split(" ")
//        .map(fieldName => StructField(fieldName, StringType, nullable = true))
//      val schema = StructType(fields)


      val x = rawData.map(r=>(r.apply(5).toString,
                              r.apply(0).toString,
                              r.apply(2).toString,
                              r.apply(8).toString,
                              r.apply(3).toString.split(" ").length.toString,
                              r.apply(3).toString.replaceAll("[^a-zA-Z]", " "))).toDF()


      val hftDataFormat = x.map(c=>(c.apply(0).toString+" "+c.apply(1).toString+" "+c.apply(2).toString+" "+c.apply(3).toString
                  +" "+c.apply(4).toString+" "+c.apply(5).toString))

      val rawDataForALS =  x.map(c=>(c.apply(0).toString, c.apply(1).toString, c.apply(2).toString, c.apply(3).toString)).toDF()

      //rawDataForALS.show(10)
      //rawDataForALS.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false").save("/home/bbkstha/CSU/I/BigData/Project/data/Musical_Instruments_5.csv")


      hftDataFormat.coalesce(1).write.text(saveDataforHFT+fileName+".txt");
      //x.show(5)



//         x.coalesce(1).write.text().format("t")sa
//          .write.format("com.databricks.spark.csv")
//          .option("header", "false")
//          .save("mydata.csv")




    }



}
