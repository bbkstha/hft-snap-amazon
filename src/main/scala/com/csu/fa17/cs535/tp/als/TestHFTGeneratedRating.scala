package com.csu.fa17.cs535.tp.als

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession



case class NewRatingClass(userId: String, itemId: String, rating: Double, timestamp: Long)
case class Ratings(userNumber: Int,itemNumber: Int, rating: Double)



object TestHFTGeneratedRating {


  def parseRating(str: String): NewRatingClass = {
    val fields = str.split(",")
    assert(fields.size == 4)
    NewRatingClass(fields(0).toString(), fields(1).toString, fields(2).toDouble, fields(3).toLong)
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ALS_Train")
      .master("local")
      .getOrCreate()
    import spark.implicits._


    val execStart = System.currentTimeMillis()
    //val fileName =args.apply(0)
    val ratingCSV = "/home/bbkstha/CSU/I/BigData/Project/data/SnapData/Rating/ratings_Electronics.csv"//"hdfs://boise:31701/ "+fileName+".txt"
    //val savedModelFile = "hdfs://boise:31701/"+fileName+"SavedModel.txt"


    val oldRatings = spark.read.textFile(ratingCSV).map(parseRating).toDF()

    //val data = spark.read.textFile(ratingCSV)
    //val tempRating = data.map(_.split(',') match { case Array(userId, itemId, rating, timestamp) =>
    //NewRatingClass(userId.toString, itemId.toString, rating.toDouble, timestamp.toLong)
    //})

    val stringindexer = new StringIndexer()
      .setInputCol("userId")
      .setOutputCol("userNumber")
    val modelc = stringindexer.fit(oldRatings)
    val  df0 = modelc.transform(oldRatings)
    val newdf0 = df0.drop("userId")

    val stringindexer1 = new StringIndexer()
      .setInputCol("itemId")
      .setOutputCol("itemNumber")
    val modelc1 = stringindexer1.fit(newdf0)
    val  df1 = modelc1.transform(newdf0)
    val ratings = df1.drop("itemId").drop("timestamp")

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setRank(50)
      .setMaxIter(10)
      .setRegParam(0.01)
      .setUserCol("userNumber")
      .setItemCol("itemNumber")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("mse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val mse = evaluator.evaluate(predictions)/10
    println(s"Mean-square error = $mse")


    /*******************************************************************/

    // Save and load model
    //model.save(savedModelFile)
    //val sameModel = MatrixFactorizationModel.load(spark.sparkContext, destinationFile)

    val execTime = (System.currentTimeMillis() - execStart)/60000.0
    println("The execution time is: "+execTime)
    spark.stop()

  }


}
