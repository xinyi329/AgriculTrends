// Look at top crop in each country based on Yield and Production

import org.apache.spark.sql.expressions.Window

val agriculTrendsDF = spark.sql("SELECT * FROM iil209.AgriculTrendsAggregation")

// Group by Country and Crop and Aggregate Sum of Yield
val totalYieldDF = agriculTrendsDF.groupBy("country", "crop").agg(sum("yield") as "total yield")
val windowSpecYield  = Window.partitionBy("country").orderBy(col("total yield").desc)

// Query for showing the crop that has the most yield in each country
val mostYieldDF = totalYieldDF.withColumn("rank",dense_rank().over(windowSpecYield)).select("country", "crop", "total yield").where("rank ==1")
mostYieldDF.write.mode(SaveMode.Overwrite).saveAsTable("iil209.AgriculTrendsMostYield")

// Query for top 10 crop yields for each country
val top10YieldDF = totalYieldDF.withColumn("row_num",row_number.over(windowSpecYield)).select("country", "crop", "total yield").where("row_num <= 10")
top10YieldDF.write.mode(SaveMode.Overwrite).saveAsTable("iil209.AgriculTrendsTop10Yield")

// Group by Country and Crop and Aggregate Sum of Production
val totalProductionDF = agriculTrendsDF.groupBy("country", "crop").agg(sum("production") as "total production")
val windowSpecProd  = Window.partitionBy("country").orderBy(col("total production").desc)

// Query for showing the crop that has the most production in each country
val mostProdDF = totalProductionDF.withColumn("rank",dense_rank().over(windowSpecProd)).select("country", "crop", "total production").where("rank ==1")
mostProdDF.write.mode(SaveMode.Overwrite).saveAsTable("iil209.AgriculTrendsMostProd")

// Query for showing the top 10 crops that have the most production in each country
val top10ProdDF =totalProductionDF.withColumn("row_num",row_number.over(windowSpecProd)).select("country", "crop", "total production").where("row_num <= 10")
top10ProdDF.write.mode(SaveMode.Overwrite).saveAsTable("iil209.AgriculTrendsTop10Prod")

// Queries for countries with most Yield, countries with most Production
val countryTotalYieldDF = agriculTrendsDF.groupBy("country").agg(sum("yield") as "total yield").orderBy(col("total yield").desc)
val countryTotalProdDF = agriculTrendsDF.groupBy("country").agg(sum("production") as "total production").orderBy(col("total production").desc)