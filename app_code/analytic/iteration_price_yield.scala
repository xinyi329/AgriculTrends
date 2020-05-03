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
 * |                 0.4956380382709309|             -1.0000000000000007|              1.0000000000000002|
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

agriculTrendsChangeRateDF.count()
/*
 * Long = 160473
 */

// Correlation

val priceChangeCorrelationDF = agriculTrendsChangeRateDF.groupBy("Country", "Crop").agg(corr("ProducerPriceChangeRate", "ProductionChangeRate").alias("ProducerPriceProductionChangeRateCorr"))
                                                        .withColumn("AbsProducerPriceProductionChangeRateCorr", abs(col("ProducerPriceProductionChangeRateCorr")))
                                                        .na.drop()

priceChangeCorrelationDF.agg(avg("AbsProducerPriceProductionChangeRateCorr"), min("ProducerPriceProductionChangeRateCorr"), max("ProducerPriceProductionChangeRateCorr")).show()

/*
 * +---------------------------------------------+------------------------------------------+------------------------------------------+
 * |avg(AbsProducerPriceProductionChangeRateCorr)|min(ProducerPriceProductionChangeRateCorr)|max(ProducerPriceProductionChangeRateCorr)|
 * +---------------------------------------------+------------------------------------------+------------------------------------------+
 * |                          0.23560043235676673|                       -1.0000000000000002|                        1.0000000000000002|
 * +---------------------------------------------+------------------------------------------+------------------------------------------+
 */

// Check outliers

agriculTrendsChangeRateDF.orderBy('ProducerPriceChangeRate.asc_nulls_last).show(100)

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

abnormalCountryYearCountDF.count()
/*
 * Long = 4875
 */

abnormalCountryYearCountDF.orderBy('ChangeRateLowCount.desc_nulls_last).show(50)

abnormalCountryYearCountDF.orderBy('ChangeRateHighCount.desc_nulls_last).show(50)

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

// val agriculTrendsChangeRateFixedDF = spark.sql("SELECT * FROM xl2700.AgriculTrendsChangeRateFixed")

// Correlation

val priceChangeCorrelationFixedDF = agriculTrendsChangeRateFixedDF.groupBy("Country", "Crop").agg(corr("ProducerPriceChangeRate", "ProductionChangeRate").alias("ProducerPriceProductionChangeRateCorr"))
                                                                  .withColumn("AbsProducerPriceProductionChangeRateCorr", abs(col("ProducerPriceProductionChangeRateCorr")))
                                                                  .na.drop()

priceChangeCorrelationFixedDF.agg(avg("AbsProducerPriceProductionChangeRateCorr"), min("ProducerPriceProductionChangeRateCorr"), max("ProducerPriceProductionChangeRateCorr")).show()
/*
 * +---------------------------------------------+------------------------------------------+------------------------------------------+
 * |avg(AbsProducerPriceProductionChangeRateCorr)|min(ProducerPriceProductionChangeRateCorr)|max(ProducerPriceProductionChangeRateCorr)|
 * +---------------------------------------------+------------------------------------------+------------------------------------------+
 * |                          0.24672729310560737|                       -1.0000000000000002|                        1.0000000000000002|
 * +---------------------------------------------+------------------------------------------+------------------------------------------+
 */

// Statistics

agriculTrendsChangeRateFixedDF.filter(col("ProducerPriceChangeRate") <= 1)
                              .withColumn("RoundProducerPriceChangeRate", round(col("ProducerPriceChangeRate"), 1))
                              .groupBy("RoundProducerPriceChangeRate").count()
                              .orderBy('RoundProducerPriceChangeRate.desc_nulls_last)
                              .show(21)
/*
 * +----------------------------+-----+                                            
 * |RoundProducerPriceChangeRate|count|
 * +----------------------------+-----+
 * |                         1.0|  852|
 * |                         0.9| 1268|
 * |                         0.8| 1672|
 * |                         0.7| 2300|
 * |                         0.6| 3029|
 * |                         0.5| 4322|
 * |                         0.4| 6306|
 * |                         0.3|10635|
 * |                         0.2|17169|
 * |                         0.1|31650|
 * |                         0.0|43854|
 * |                        -0.1|14528|
 * |                        -0.2| 7168|
 * |                        -0.3| 3848|
 * |                        -0.4| 1770|
 * |                        -0.5|  861|
 * |                        -0.6|  428|
 * |                        -0.7|  183|
 * |                        -0.8|   91|
 * |                        -0.9|   29|
 * |                        -1.0|    3|
 * +----------------------------+-----+
 */

val lowerProducerPriceCount = agriculTrendsChangeRateFixedDF.filter(col("ProducerPriceChangeRate") < -0.5).count()

val lowerProducerPriceHigherProductionCount = agriculTrendsChangeRateFixedDF.filter(col("ProducerPriceChangeRate") < -0.5 && col("ProductionChangeRate") > 0).count()

val lowerProducerPriceHigherProductionRate = lowerProducerPriceHigherProductionCount.toFloat / lowerProducerPriceCount.toFloat
/*
 * Float = 0.63703704
 */

agriculTrendsChangeRateFixedDF.filter(col("ProducerPriceChangeRate") < -0.5 && col("ProductionChangeRate") > 0)
                              .withColumn("RoundProductionChangeRate", round(col("ProductionChangeRate"), 1))
                              .groupBy("RoundProductionChangeRate").count()
                              .orderBy('RoundProductionChangeRate.desc_nulls_last)
                              .show(100)
/*
 * +-------------------------+-----+                                               
 * |RoundProductionChangeRate|count|
 * +-------------------------+-----+
 * |                     13.2|    1|
 * |                     11.0|    1|
 * |                      9.6|    1|
 * |                      8.6|    1|
 * |                      7.2|    1|
 * |                      7.0|    1|
 * |                      5.9|    1|
 * |                      5.3|    1|
 * |                      4.8|    1|
 * |                      4.4|    1|
 * |                      4.3|    1|
 * |                      3.9|    1|
 * |                      3.8|    1|
 * |                      3.2|    2|
 * |                      3.0|    1|
 * |                      2.6|    1|
 * |                      2.5|    1|
 * |                      2.4|    2|
 * |                      2.1|    5|
 * |                      2.0|    2|
 * |                      1.9|    5|
 * |                      1.8|    3|
 * |                      1.7|    4|
 * |                      1.6|    4|
 * |                      1.5|    2|
 * |                      1.4|    8|
 * |                      1.3|    9|
 * |                      1.2|    2|
 * |                      1.1|    2|
 * |                      1.0|   10|
 * |                      0.9|   13|
 * |                      0.8|   23|
 * |                      0.7|   20|
 * |                      0.6|   21|
 * |                      0.5|   35|
 * |                      0.4|   25|
 * |                      0.3|   70|
 * |                      0.2|   97|
 * |                      0.1|  177|
 * |                      0.0|  131|
 * +-------------------------+-----+
 */
