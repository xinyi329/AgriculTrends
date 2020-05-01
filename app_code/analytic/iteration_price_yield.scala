import org.apache.spark.sql.SaveMode

val agriculTrendsDF = spark.sql("SELECT * FROM xl2700.AgriculTrendsAggregation")

// Correlation

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


