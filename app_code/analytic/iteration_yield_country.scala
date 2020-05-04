// Look at top crop in each country based on Yield and Production

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode

val agriculTrendsDF = spark.sql("SELECT * FROM iil209.AgriculTrendsAggregation")

// Group by Country and Crop and Aggregate Sum of Yield
val totalYieldDF = agriculTrendsDF.groupBy("country", "crop").agg(sum("yield") as "totalYield")
val windowSpecYield  = Window.partitionBy("country").orderBy(col("totalYield").desc)

// Query for showing the crop that has the most yield in each country
val mostYieldDF = totalYieldDF.withColumn("rank",dense_rank().over(windowSpecYield)).select("country", "crop", "totalYield").where("rank ==1")
mostYieldDF.write.mode(SaveMode.Overwrite).saveAsTable("iil209.AgriculTrendsMostYield")

// Query for top 10 crop yields for each country
val top10YieldDF = totalYieldDF.withColumn("row_num",row_number.over(windowSpecYield)).select("country", "crop", "totalYield").where("row_num <= 10")
top10YieldDF.write.mode(SaveMode.Overwrite).saveAsTable("iil209.AgriculTrendsTop10Yield")

// Group by Country and Crop and Aggregate Sum of Production
val totalProductionDF = agriculTrendsDF.groupBy("country", "crop").agg(sum("production") as "totalProduction")
val windowSpecProd  = Window.partitionBy("country").orderBy(col("totalProduction").desc)

// Query for showing the crop that has the most production in each country
val mostProdDF = totalProductionDF.withColumn("rank",dense_rank().over(windowSpecProd)).select("country", "crop", "totalProduction").where("rank ==1")
mostProdDF.write.mode(SaveMode.Overwrite).saveAsTable("iil209.AgriculTrendsMostProd")

// Query for showing the top 10 crops that have the most production in each country
val top10ProdDF =totalProductionDF.withColumn("row_num",row_number.over(windowSpecProd)).select("country", "crop", "totalProduction").where("row_num <= 10")
top10ProdDF.write.mode(SaveMode.Overwrite).saveAsTable("iil209.AgriculTrendsTop10Prod")

// Query for total crop yield in each country
val countryTotalYieldDF = agriculTrendsDF.groupBy("country").agg(sum("yield") as "totalYield").orderBy(col("totalYield").desc)
countryTotalYieldDF.write.mode(SaveMode.Overwrite).saveAsTable("iil209.AgriculTrendsTotalYield")

// Query for total crop production quantity in each country
val countryTotalProdDF = agriculTrendsDF.groupBy("country").agg(sum("production") as "totalProduction").orderBy(col("totalProduction").desc)
countryTotalProdDF.write.mode(SaveMode.Overwrite).saveAsTable("iil209.AgriculTrendsTotalProd")

// Since Denmark has the highest total yield, we look at its crop which has the highest yield:
mostYieldDF.filter($"country".contains("Denmark")).show()
// +-------+-------------+-------------+                                           
// |country|         crop|   totalYield|
// +-------+-------------+-------------+
// |Denmark|Fruit Primary|2.744245847E9|
// +-------+-------------+-------------+

// Since China has the highest total production quantity, we look at its crop which has the highest production:
mostProdDF.filter($"country".contains("China")).show()
// +-------+-------------+---------------+                                         
// |country|         crop|totalProduction|
// +-------+-------------+---------------+
// |  China|Cereals Total|1.7102802525E10|
// +-------+-------------+---------------+
