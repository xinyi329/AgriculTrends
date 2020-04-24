import org.apache.spark.sql.SaveMode

val agriculTrendsDF = spark.sql("SELECT * FROM xl2700.AgriculTrendsAggregation")

// Correlation

val yieldCorrelationDF = agriculTrendsDF.groupBy("Crop").agg(corr("Yield", "Precipitation").alias("YieldPrecipitationCorr"))
                                        .join(agriculTrendsDF.groupBy("Crop").agg(corr("Yield", "Temperature").alias("YieldTemperatureCorr")), Seq("Crop"), "inner")
                                        .withColumn("AbsYieldPrecipitationCorr", abs(col("YieldPrecipitationCorr")))
                                        .withColumn("AbsYieldTemperatureCorr", abs(col("YieldTemperatureCorr")))
                                        .withColumn("MaxAbsYieldCorr", greatest(col("AbsYieldPrecipitationCorr"), col("AbsYieldTemperatureCorr")))

yieldCorrelationDF.write.mode(SaveMode.Overwrite).saveAsTable("xl2700.AgriculTrendsYieldCorrelation")

yieldCorrelationDF.agg(avg("AbsYieldPrecipitationCorr"), avg("AbsYieldTemperatureCorr"), avg("MaxAbsYieldCorr"), max("AbsYieldPrecipitationCorr"), max("AbsYieldTemperatureCorr")).show()
/*
 * +------------------------------+----------------------------+--------------------+------------------------------+----------------------------+
 * |avg(AbsYieldPrecipitationCorr)|avg(AbsYieldTemperatureCorr)|avg(MaxAbsYieldCorr)|max(AbsYieldPrecipitationCorr)|max(AbsYieldTemperatureCorr)|
 * +------------------------------+----------------------------+--------------------+------------------------------+----------------------------+
 * |           0.15804906228317994|         0.21716067188378688| 0.26076262461768784|            0.8571434541498721|          0.8448481150581659|
 * +------------------------------+----------------------------+--------------------+------------------------------+----------------------------+
 */

val priceCorrelationDF = agriculTrendsDF.groupBy("Crop").agg(corr("ProducerPrice", "Production").alias("ProducerPriceProductionCorr"))
                                        .withColumn("AbsProducerPriceProductionCorr", abs(col("ProducerPriceProductionCorr")))

priceCorrelationDF.write.mode(SaveMode.Overwrite).saveAsTable("xl2700.AgriculTrendsPriceCorrelation")

priceCorrelationDF.agg(avg("AbsProducerPriceProductionCorr"), min("ProducerPriceProductionCorr"), max("ProducerPriceProductionCorr")).show()
/*
 * +-----------------------------------+--------------------------------+--------------------------------+
 * |avg(AbsProducerPriceProductionCorr)|min(ProducerPriceProductionCorr)|max(ProducerPriceProductionCorr)|
 * +-----------------------------------+--------------------------------+--------------------------------+
 * |                0.11220331921783815|             -0.6867613149891504|              0.7627345152839774|
 * +-----------------------------------+--------------------------------+--------------------------------+
 */

// Heat Map - Yield vs. Precipitation & Temperature by Crop

val yieldHeatMapDF = agriculTrendsDF.withColumn("RoundPrecipitation", floor(col("Precipitation")))
                                    .withColumn("RoundTemperature", floor(col("Temperature")))
                                    .groupBy("Crop", "RoundPrecipitation", "RoundTemperature").agg(sum("Yield").alias("YieldSum"), count(lit(1)).alias("YieldRecordNum"), avg("Yield").alias("YieldAvg"))

yieldHeatMapDF.write.mode(SaveMode.Overwrite).saveAsTable("xl2700.AgriculTrendsYieldHeatMap")
