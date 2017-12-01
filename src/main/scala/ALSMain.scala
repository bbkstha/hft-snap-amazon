
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession


  object ALSMain {

    case class NewRatingClass(userId: String, itemId: String, rating: Double, timestamp: Long)
    case class Ratings(userNumber: Int,itemNumber: Int, rating: Double)


    def parseRating(str: String): NewRatingClass = {
      val fields = str.split(",")
      assert(fields.size == 4)
      NewRatingClass(fields(0).toString(), fields(1).toString, fields(2).toDouble, fields(3).toLong)
    }

    def main(args: Array[String]): Unit = {

      val spark = SparkSession
        .builder()
        .appName("ALS Test")
        .master("local")
        .getOrCreate()
      import spark.implicits._

      val ratingCSV = "/home/bbkstha/CSU/I/Big Data/Project/data/ratings_Digital_Music.csv"
      val destinationFile = "/home/bbkstha/CSU/I/Big Data/Project/data/SavedModel"


      val oldRatings = spark.read.textFile(ratingCSV).map(parseRating).toDF()

      val data = spark.read.textFile(ratingCSV)



      val tempRating = data.map(_.split(',') match { case Array(userId, itemId, rating, timestamp) =>
        NewRatingClass(userId.toString, itemId.toString, rating.toDouble, timestamp.toLong)
      })

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
      val newdf1 = df1.drop("itemId").drop("timestamp")


      val ratings = newdf1//.as[Ratings]



    //ratings.show()


      val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
      //training.show()
      //test.show()

      // Build the recommendation model using ALS on the training data
      val als = new ALS()
        .setMaxIter(2)
        .setRegParam(0.01)
        .setUserCol("userNumber")
        .setItemCol("itemNumber")
        .setRatingCol("rating")
      val model = als.fit(training)
//
//
//      // Evaluate the model by computing the RMSE on the test data
//      // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
      model.setColdStartStrategy("drop")
      val predictions = model.transform(test)

      predictions.show();


      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")
      val rmse = evaluator.evaluate(predictions)
      println(s"Root-mean-square error = $rmse")


      /*******************************************************************/

      // Generate top 10 movie recommendations for each user
      val userRecs = model.recommendForAllUsers(10)
     // userRecs.show()
      // Generate top 10 user recommendations for each movie
      val movieRecs = model.recommendForAllItems(10)
     // movieRecs.show()
      spark.stop()


    }

  // scalastyle:on println

}
