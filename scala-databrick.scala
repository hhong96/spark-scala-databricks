
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window


spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))


val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")


val customSchema2 = StructType(Array(StructField("LocationID", IntegerType, true), StructField("Borough", StringType, true), StructField("Zone", StringType, true), StructField("service_zone", StringType, true)))

val df2 = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema2)
   .load("/FileStore/tables/taxi_zone_lookup.csv") // the csv file which you want to work with


val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)

df_filter.show(5)

var df_top_do = df_filter.groupBy("DOLocationID").count()
                         .select(col("DOLocationID"), col("count").alias("number_of_dropoffs").cast(IntegerType)).orderBy(col("number_of_dropoffs").desc, col("DOLocationID").asc).limit(5)

var df_top_pu = df_filter.groupBy("PULocationID").count()
                         .select(col("PULocationID"), col("count").alias("number_of_pickups").cast(IntegerType)).orderBy(col("number_of_pickups").desc, col("PULocationID").asc).limit(5)


df_pu = df_filter.select(col("PULocationID").alias("LocationID"), col("passenger_count")).groupBy("LocationID").count().select(col("LocationID"), col("count").alias("PUCount"))
df_do = df_filter.select(col("DOLocationID").alias("LocationID"), col("passenger_count")).groupBy("LocationID").count().select(col("LocationID"), col("count").alias("DOCount"))

var df_top_loc = df_pu.join(df_do, Seq("LocationID"))
                      .select(col("LocationID"), (col("PUCount")+col("DOCount")).alias("number_activities").cast(IntegerType)).orderBy(col("number_activities").desc, col("LocationID").asc).limit(3)


var df_bor = df_top_loc.join(df2, Seq("LocationID")).groupBy("Borough").sum("number_activities")
                        .select(col("Borough"), col("sum(number_activities)").alias("total_number_activities").cast(IntegerType)).orderBy(col("total_number_activities").desc)

var df_dow = df_filter.withColumn("date", date_format(col("pickup_datetime"), "MM-dd-yyyy")).groupBy("date").count()
                      .withColumn("day_of_week", date_format(to_date(col("date"), "MM-dd-yyyy"), "E")).groupBy("day_of_week").avg()
                      .select(col("day_of_week"), col("avg(count)").cast(FloatType).alias("avg_count")).orderBy(col("avg_count").desc).limit(2)

var windowSpec = Window.partitionBy("hour_of_day").orderBy(col("max_count").desc)

var df_hod = df_filter.withColumn("hour_of_day", hour(col("pickup_datetime")))
                      .groupBy(col("PULocationID"), col("hour_of_day")).count()
                      .select(col("PULocationID").alias("LocationID"), col("hour_of_day"), col("count").alias("max_count"))
                      .join(df2, Seq("LocationID")).filter(col("Borough") === "Brooklyn")
                      .withColumn("top_count", max("max_count") over windowSpec)
                      .filter(col("max_count") === col("top_count"))
                      .select(col("hour_of_day"), col("zone"), col("max_count").cast(IntegerType)).orderBy(col("hour_of_day"))


var w = Window.orderBy("day")
var df_jan = df_filter.select(date_format(col("pickup_datetime"), "dd").alias("day"), date_format(col("pickup_datetime"), "MMM").alias("Month")).filter(col("Month") === "Jan").drop(col("Month"))
                      .groupBy(col("day")).count()
                      .withColumn("percent_change", round((col("count")/lag("count", 1, 0).over(w))-1, 2).cast(FloatType)).drop(col("count"))
                      .select(col("day").cast(IntegerType), col("percent_change")).orderBy(col("percent_change").desc).limit(3)
