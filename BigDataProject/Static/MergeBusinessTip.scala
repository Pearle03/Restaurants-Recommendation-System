import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// Initialize Spark session
val spark = SparkSession.builder.appName("Yelp Data Processing").getOrCreate()
import spark.implicits._

// Define schema for business and tip data
val businessSchema = StructType(Array(StructField("businessId", StringType, true)))
val tipSchema = StructType(Array(StructField("businessId", StringType, true), StructField("tip", StringType, true)
))

// Read business data with schema
val businessData = spark.read
  .option("header", "true")
  .schema(businessSchema)
  .csv("yelp_academic_dataset_business.csv")

// Read tip data with schema
val tipData = spark.read
  .option("header", "true")
  .schema(tipSchema)
  .csv("yelp_academic_dataset_tip.csv")

// Join the datasets on businessId and remove duplicates
val joinedDf = businessData.join(tipData, "businessId").distinct()

// Select and format the columns for the final output
val formattedDf = joinedDf
  .select("businessId", /* other columns */)
  .map(row => row.mkString(","))

// Write the output
formattedDf.write.text("output_final")
