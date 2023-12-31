import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel}

val spark = SparkSession.builder()
  .appName("Yelp Recommendation System")
  .getOrCreate()
import spark.implicits._

val type_of_restaurant = readLine("Enter type of Food:").toLowerCase
val city = readLine("Enter City:").toLowerCase
val state = readLine("Enter State:").toLowerCase
val user_id = readLine("Enter User ID:")

val businessDataPath = "yelp_academic_dataset_business.csv"
val reviewDataPath = "yelp_academic_dataset_review.csv"
val outputPath = ""

val businessDF = spark.read
  .option("header", "true")
  .csv(businessDataPath)
  .select(lower(col("categories")).alias("categories"), col("business_id"), col("name"), col("stars"))
  .filter(col("categories").contains("restaurant") &&
    col("categories").contains(type_of_restaurant) &&
    lower(col("city")) === city &&
    lower(col("state")) === state)

val reviewDF = spark.read
  .option("header", "true")
  .csv(reviewDataPath)
  .withColumn("dateReviewed", unix_timestamp(col("date"), "yyyy-MM-dd"))
  .filter(col("business_id").isin(businessDF.select("business_id").as[String].collect(): _*))


// Check if user has reviewed any businesses
val userReviewed = reviewDF.filter(col("user_id") === user_id).count() > 0

// Convert review data to RDD
val ratingsRDD = reviewDF
  .rdd
  .map { row =>
    val uid = row.getString(0).replace("[", "")
    val review_bid = row.getString(3)
    val rating = row.getFloat(5)
    val dateReviewed = row.getLong(7)
    Rating(uid.toInt, review_bid.toInt, rating.toDouble)
  }

// Train an ALS model if the user has reviewed businesses
if (userReviewed) {
  val rank = 5
  val numIterations = 10
  val model: MatrixFactorizationModel = new ALS()
    .setRank(rank)
    .setIterations(numIterations)
    .setLambda(0.01)
    .run(ratingsRDD)

  // Generate recommendations for the user
  val userProducts = ratingsRDD.map {
    case Rating(user, product, _) =>
      (user, product)
  }
  val predictions = model.predict(userProducts).map {
    case Rating(user, product, rate) =>
      ((user, product), rate)
  }
  val top5Recommendations = predictions.top(5)(Ordering.by(_._2))

  println("Top 5 Restaurants:")
  top5Recommendations.foreach {
    case ((user, businessID), rate) =>
      println(s"$businessID $rate")
  }
} else {
  // Display top 5 restaurants based on the search criteria
  val top5Restaurants = businessDF.orderBy(col("stars").desc).limit(5)
  println("Top 5 Restaurants based on the search:")
  top5Restaurants.show()
}

spark.stop()
