import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import java.text.SimpleDateFormat

val spark = SparkSession.builder()
  .appName("Yelp Recommendation System")
  .getOrCreate()

val type_of_restaurant = readLine("Enter type of Food:").toLowerCase
val city = readLine("Enter City:").toLowerCase
var residingState = readLine("Enter State:").toLowerCase
val user_id = readLine("Enter User ID:")

val businessDataPath = "yelp_academic_dataset_business.csv"
val reviewDataPath = "yelp_academic_dataset_review.csv"
val outputPath = "/"

residingState = residingState.toLowerCase

val businessDF = spark.read
  .option("header", "true")
  .option("parserLib", "UNIVOCITY")
  .csv(businessDataPath)
  .select(lower(col("categories")).alias("categories"), col("business_id"), col("name"), col("stars"))
  .filter(col("categories").contains("restaurant") &&
    col("categories").contains(type_of_restaurant) &&
    col("city") === city &&
    col("state") === residingState)

val restaurantList = businessDF
  .rdd
  .map(row => row.getString(1))
  .collect()

val ResNameRat = businessDF
  .rdd
  .map(row => (row.getString(2), row.getDouble(3)))
  .collectAsMap()

val reviewDF = spark.read
  .option("header", "true")
  .option("parserLib", "UNIVOCITY")
  .csv(reviewDataPath)
  .withColumn("dateReviewed", unix_timestamp(col("date"), "yyyy-MM-dd"))
  .filter(col("business_id").isin(restaurantList: _*))

val userReviewed = reviewDF
  .filter(col("user_id") === user_id)
  .count() > 0

val hp = reviewDF
  .rdd
  .map { row =>
    val uid = row.getString(0).replace("[", "")
    val review_bid = row.getString(3)
    val rating = row.getFloat(5)
    val dateReviewed = row.getLong(7)
    (uid, review_bid, rating, dateReviewed)
  }
  .aggregateByKey((0.0, 0L))(
    (acc, value) => (acc._1 + value._3, Math.max(acc._2, value._4)),
    (acc1, acc2) => (acc1._1 + acc2._1, Math.max(acc1._2, acc2._2))
  )
  .map {
    case (uid, review_bid, totalRating, maxDate) =>
      val uid_bid = s"$uid|$review_bid"
      val rating_date = s"${totalRating.toFloat}|$maxDate"
      (uid_bid, rating_date)
  }
  .collectAsMap()

val userIdToNum = new util.HashMap[String, Int]()
val businessIdToNum = new util.HashMap[String, Int]()
var usercount = 2

for (i <- hp.keySet().toArray()) {
  val tempArray = i.toString().split("\\|")
  val tempuid = tempArray(0)
  val tempbid = tempArray(1)

  userIdToNum.put(tempuid, if (tempuid == user_id) 1 else usercount)
  businessIdToNum.put(tempbid, usercount)
  usercount += 1
}

val ALSList = hp.keySet().toArray().map { i =>
  val tempArray = i.toString().split("\\|")
  val temp = s"${userIdToNum.get(tempArray(0))}::${businessIdToNum.get(tempArray(1))}"
  val valueArray = hp.get(i).toString().split("\\|")
  s"$temp::${valueArray(0)}::${valueArray(1)}"
}

val testHM = businessIdToNum.keySet().toArray().map { i =>
  s"1::${businessIdToNum.get(i)}::0"
}

sc.parallelize(ALSList, 1).saveAsTextFile(s"$outputPath/RatingData9.dat")
sc.parallelize(testHM, 1).saveAsTextFile(s"$outputPath/TestRatingData9.dat")

if (userReviewed) {
  val data = sc.textFile(s"$outputPath/RatingData9.dat/part-00000")
  val ratings = data.map(_.split("::") match {
    case Array(userID, businessID, rating, timestamp) =>
      Rating(userID.toInt, businessID.toInt, rating.toDouble)
  })
  val testData = sc.textFile(s"$outputPath/TestRatingData9.dat/part-00000")
  val testing = testData.map(_.split("::") match {
    case Array(userID, businessID, rating) =>
      Rating(userID.toInt, businessID.toInt, rating.toDouble)
  })
  val rank = 5
  val numIterations = 10
  val model = ALS.train(ratings, rank, numIterations, 0.01)
  val usersProducts = testing.map {
    case Rating(user, product, ratings) =>
      (user, product)
  }
  val predictions = model.predict(usersProducts).map {
    case Rating(user, product, rate) =>
      ((user, product), rate)
  }
  val l = predictions.sortBy(-_._2).take(5)
  val Top5Rest = l.map { case ((_, businessID), rate) => businessIdToNum.inverse.get(businessID).toString -> rate }
  println("Top 5 Restaurants:")
  Top5Rest.foreach {
    case (businessName, rate) =>
      println(s"${ResNameRat(businessName)} $rate")
  }
} else {
  val ratMapSeq = ResNameRat.toSeq.sortBy(-_._2).take(5)
  println("Top 5 Restaurants based on the search is ->")
  ratMapSeq.foreach {
    case (businessName, rate) =>
      println(s"$businessName $rate")
  }
}
spark.stop()
