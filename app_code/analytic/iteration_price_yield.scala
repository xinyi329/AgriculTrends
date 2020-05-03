import org.apache.spark.sql.SaveMode

val agriculTrendsDF = spark.sql("SELECT * FROM xl2700.AgriculTrendsAggregation").na.drop()

// Correlation

val priceCorrelationDF = agriculTrendsDF.groupBy("Country", "Crop").agg(corr("ProducerPrice", "Production").alias("ProducerPriceProductionCorr"))
                                        .withColumn("AbsProducerPriceProductionCorr", abs(col("ProducerPriceProductionCorr")))
                                        .na.drop()

priceCorrelationDF.agg(avg("AbsProducerPriceProductionCorr"), min("ProducerPriceProductionCorr"), max("ProducerPriceProductionCorr")).show()
/*
 * +-----------------------------------+--------------------------------+--------------------------------+
 * |avg(AbsProducerPriceProductionCorr)|min(ProducerPriceProductionCorr)|max(ProducerPriceProductionCorr)|
 * +-----------------------------------+--------------------------------+--------------------------------+
 * |                 0.4956380382709307|             -1.0000000000000007|              1.0000000000000002|
 * +-----------------------------------+--------------------------------+--------------------------------+
 */

// Rate of change

val agriculTrendsLastYearDF = agriculTrendsDF.withColumnRenamed("Year", "LastYear")
                                             .withColumnRenamed("Production", "LastYearProduction")
                                             .withColumnRenamed("ProducerPrice", "LastYearProducerPrice")
                                             .select("Country", "Crop", "LastYear", "LastYearProduction", "LastYearProducerPrice")

val agriculTrendsChangeRateDF = agriculTrendsDF.withColumn("LastYear", col("Year") - 1)
                                               .join(agriculTrendsLastYearDF, Seq("Country", "Crop", "LastYear"), "inner")
                                               .withColumn("ProductionChangeRate", (col("Production") - col("LastYearProduction")) / col("LastYearProduction"))
                                               .withColumn("ProducerPriceChangeRate", (col("ProducerPrice") - col("LastYearProducerPrice")) / col("LastYearProducerPrice"))
                                               .select("Country", "Crop", "Year", "Production", "ProductionChangeRate", "ProducerPrice", "ProducerPriceChangeRate")

agriculTrendsChangeRateDF.write.mode(SaveMode.Overwrite).saveAsTable("xl2700.AgriculTrendsChangeRate")

agriculTrendsChangeRateDF.orderBy('ProducerPriceChangeRate.asc_nulls_last).show(100)

agriculTrendsChangeRateDF.count()
/*
 * Long = 160473
 */

// Filter out abnormal data points: for each country and year, most < -80% or most > 100%

val abnormalChangeRateDF = agriculTrendsChangeRateDF.select("Country", "Crop", "Year", "ProducerPriceChangeRate").na.drop()
                                                    .withColumn("ChangeRateLow", when(col("ProducerPriceChangeRate") < -0.8, true).otherwise(false))
                                                    .withColumn("ChangeRateHigh", when(col("ProducerPriceChangeRate") > 1, true).otherwise(false))

val abnormalCountryYearCountDF = abnormalChangeRateDF.groupBy("Country", "Year").agg(count("Crop").alias("Count"))
                                                     .join(abnormalChangeRateDF.filter(col("ChangeRateLow") === true)
                                                                               .groupBy("Country", "Year").agg(count("ChangeRateLow").alias("ChangeRateLowCount")),
                                                           Seq("Country", "Year"), "left")
                                                     .join(abnormalChangeRateDF.filter(col("ChangeRateHigh") === true)
                                                                               .groupBy("Country", "Year").agg(count("ChangeRateHigh").alias("ChangeRateHighCount")),
                                                           Seq("Country", "Year"), "left")
                                                     .na.fill(0)

abnormalCountryYearCountDF.orderBy('ChangeRateLowCount.desc_nulls_last).show(50)

abnormalCountryYearCountDF.orderBy('ChangeRateHighCount.desc_nulls_last).show(50)

abnormalCountryYearCountDF.count()
/*
 * Long = 4875
 */

val normalCountryYearDF = abnormalCountryYearCountDF.filter(col("ChangeRateLowCount") / col("Count") < 0.9 && col("ChangeRateHighCount") / col("Count") < 0.9)
                                                    .select("Country", "Year")

normalCountryYearDF.count()
/*
 * Long = 4783
 */

val agriculTrendsChangeRateFixedDF = normalCountryYearDF.join(agriculTrendsChangeRateDF, Seq("Country", "Year"), "leftouter").na.drop()

agriculTrendsChangeRateFixedDF.write.mode(SaveMode.Overwrite).saveAsTable("xl2700.AgriculTrendsChangeRateFixed")

agriculTrendsChangeRateFixedDF.count()
/*
 * Long = 157067
 */

// Correlation

val priceChangeCorrelationFixedDF = agriculTrendsChangeRateFixedDF.groupBy("Country", "Crop").agg(corr("ProducerPriceChangeRate", "ProductionChangeRate").alias("ProducerPriceProductionChangeRateCorr"))
                                                                  .withColumn("AbsProducerPriceProductionChangeRateCorr", abs(col("ProducerPriceProductionChangeRateCorr")))
                                                                  .na.drop()

priceChangeCorrelationFixedDF.agg(avg("AbsProducerPriceProductionChangeRateCorr"), min("ProducerPriceProductionChangeRateCorr"), max("ProducerPriceProductionChangeRateCorr")).show()
/*
 * +---------------------------------------------+------------------------------------------+------------------------------------------+
 * |avg(AbsProducerPriceProductionChangeRateCorr)|min(ProducerPriceProductionChangeRateCorr)|max(ProducerPriceProductionChangeRateCorr)|
 * +---------------------------------------------+------------------------------------------+------------------------------------------+
 * |                           0.2467272931056066|                       -1.0000000000000002|                        1.0000000000000002|
 * +---------------------------------------------+------------------------------------------+------------------------------------------+
 */

 // Statistics TBC
