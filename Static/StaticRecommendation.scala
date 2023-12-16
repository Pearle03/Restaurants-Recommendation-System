import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import scala.collection.JavaConverters._

val spark = SparkSession.builder()
  .appName("Yelp Recommendation System")
  .getOrCreate()
import spark.implicits._

// User inputs
val typeOfRestaurant = readLine("Enter type of Food:").toLowerCase
val city = readLine("Enter City:").toLowerCase
val residingState = readLine("Enter State:").toLowerCase
val userId = readLine("Enter User ID:")

// Paths
val businessDataPath = "yelp_academic_dataset_business.csv"
val reviewDataPath = "yelp_academic_dataset_review.csv"
val outputPath = "/"

// Read and filter business data
val businessDF = spark.read
  .option("header", "true")
  .csv(businessDataPath)
  .filter($"categories".contains("restaurant") &&
    lower($"categories").contains(typeOfRestaurant) &&
    lower($"city") === city &&
    lower($"state") === residingState)
  .select($"business_id", $"name", $"stars")

// Read and filter review data
val reviewDF = spark.read
  .option("header", "true")
  .csv(reviewDataPath)
  .filter($"business_id".isin(businessDF.select("business_id").as[String].collect(): _*))

// Check if user has reviewed
val userReviewed = reviewDF.filter($"user_id" === userId).count() > 0

// Prepare data for ALS
val ratingsRDD = reviewDF.as[(String, String, String, Float)]
  .map{ case (user, _, business, rating) => Rating(user.hashCode, business.hashCode, rating) }
  .rdd

// ALS Model Training
val rank = 5
val numIterations = 10
val model = ALS.train(ratingsRDD, rank, numIterations, 0.01)

if (userReviewed) {
  // Prediction for a user
  val userProducts = businessDF.select($"business_id").distinct
    .map(_.getString(0).hashCode)
    .map((userId.hashCode, _))
    .rdd

  val predictions = model.predict(userProducts)
    .map(r => (r.product, r.rating))
    .join(businessDF.rdd.map(r => (r.getString(0).hashCode, r.getString(1))))
    .map(_._2)
    .sortBy(-_._2)
    .take(5)

  println("Top 5 Recommended Restaurants:")
  predictions.foreach { case (name, rating) => println(s"$name -> Rating: $rating") }
} else {
  // Top 5 Restaurants based on overall ratings
  val topRestaurants = businessDF.sort($"stars".desc).take(5)
  println("Top 5 Restaurants based on overall ratings:")
  topRestaurants.foreach(r => println(s"${r.getString(1)} -> Rating: ${r.getDouble(2)}"))
}

spark.stop()
