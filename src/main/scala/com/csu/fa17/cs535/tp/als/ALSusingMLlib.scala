//package com.csu.fa17.cs535.tp.als
//
//
//
//
//
//  /*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//  // scalastyle:off println
//
//
//  import com.csu.fa17.cs535.tp.als.TestHFTGeneratedRating.parseRating
//  import org.apache.spark.ml.feature.StringIndexer
//  import org.apache.spark.{SparkConf, SparkContext}
//  // $example on$
//  import org.apache.spark.mllib.recommendation.ALS
//  import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
//  import org.apache.spark.mllib.recommendation.Rating
//  // $example off$
//
//  object ALSusingMLlib {
//
//
//    def main(args: Array[String]): Unit = {
//
//      val conf = new SparkConf().setAppName("CollaborativeFilteringExample")
//      val sc = new SparkContext(conf)
//
//      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//
//      // this is used to implicitly convert an RDD to a DataFrame.
//      import sqlContext.implicits._
//
//      // $example on$
//      // Load and parse the data
//      val data = sc.textFile("/home/bbkstha/CSU/I/BigData/Project/data" +
//                              "/SnapData/Rating/ratings_Musical_Instruments.csv").map(parseRating).toDF()
//
//
//      val stringindexer = new StringIndexer()
//        .setInputCol("userId")
//        .setOutputCol("userNumber")
//      val modelc = stringindexer.fit(data)
//      val  df0 = modelc.transform(data)
//      val newdf0 = df0.drop("userId")
//
//      val stringindexer1 = new StringIndexer()
//        .setInputCol("itemId")
//        .setOutputCol("itemNumber")
//      val modelc1 = stringindexer1.fit(newdf0)
//      val  df1 = modelc1.transform(newdf0)
//      val rating = df1.drop("itemId").drop("timestamp")
//
//
//
//      val ratings = rating match { case Array(userNumber, itemNumber, rating) =>
//        Rating(userNumber.toInt, itemNumber.toInt, rating.toDouble)
//      })
//
//      // Build the recommendation model using ALS
//      val rank = 10
//      val numIterations = 10
//      val model = ALS.train(ratings, rank, numIterations, 0.01)
//
//      // Evaluate the model on rating data
//      val usersProducts = ratings.map { case Rating(user, product, rate) =>
//        (user, product)
//      }
//      val predictions =
//        model.predict(usersProducts).map { case Rating(user, product, rate) =>
//          ((user, product), rate)
//        }
//      val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
//        ((user, product), rate)
//      }.join(predictions)
//      val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
//        val err = (r1 - r2)
//        err * err
//      }.mean()
//      println("Mean Squared Error = " + MSE)
//
//      // Save and load model
//      //###//model.save(sc, "target/tmp/myCollaborativeFilter")
//      //###//val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
//      // $example off$
//
//      sc.stop()
//    }
//  }
//  // scalastyle:on println
//
//
//
//
//
//
//
