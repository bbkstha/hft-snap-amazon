package com.csu.fa17.cs535.tp
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession

object ALSModelLoad {

  def main(args: Array[String]) {


    val spark = SparkSession
      .builder()
      .appName("ALS Test")
      .master("local")
      .getOrCreate()
    val fileName =args.apply(0)
    val savedModelFile = "hdfs://boise:31701/"+fileName+"SavedModel.txt"
    val savedModel = MatrixFactorizationModel.load(spark.sparkContext, savedModelFile)
    val predictions = savedModel.predict(args.apply(1).toString.toInt, args.apply(2).toString.toInt)
    println("The predicted rating is %d"+predictions)
    spark.stop()

  }
}