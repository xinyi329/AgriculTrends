import org.apache.spark.sql.SaveMode

val countryCodeDF = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:/user/xl2700/agricultrends/FAOCountryCode.csv")
                         .withColumnRenamed("Short name", "Country").select("Country", "ISO3", "FAOSTAT")
val precipitationDF = spark.read.option("inferSchema", "true").csv("hdfs:/user/xl2700/agricultrends/climate/precipitation_etl/")
                           .toDF("ISO3", "Year", "Precipitation")
val temperatureDF = spark.read.option("inferSchema", "true").csv("hdfs:/user/xl2700/agricultrends/climate/temperature_etl/")
                         .toDF("ISO3", "Year", "Temperature")
val productionDF = spark.read.option("inferSchema", "true").csv("hdfs:/user/yl6183/BDAD_project/cleaned_data_v2")
                        .toDF("CountryCode", "Country", "CropCode", "Crop", "Year", "AreaHarvested", "Yield", "Production")
val producerPriceDF = spark.read.option("inferSchema", "true").csv("hdfs:/user/iil209/bdad_project/cleaned_data_combined_v2")
                           .toDF("CountryCode", "Country", "CropCode", "Crop", "Year", "ProducerPrice")

val weatherDF = precipitationDF.join(temperatureDF, Seq("ISO3", "Year"), "outer")
val cropDF = productionDF.join(producerPriceDF.select("CountryCode", "CropCode", "Year", "ProducerPrice"), Seq("CountryCode", "CropCode", "Year"), "left")
                         .select("CountryCode", "Crop", "Year", "AreaHarvested", "Yield", "Production", "ProducerPrice")
val agriculTrendsDF = cropDF.join(countryCodeDF, cropDF("CountryCode") === countryCodeDF("FAOSTAT"), "left").join(weatherDF, Seq("ISO3", "Year"), "left")
                            .select("Country", "Crop", "Year", "AreaHarvested", "Yield", "Production", "ProducerPrice", "Precipitation", "Temperature")
                            .na.drop(Seq("Country", "Crop", "Year", "AreaHarvested", "Yield", "Production", "Precipitation", "Temperature"))

val countryListDF = agriculTrendsDF.select("Country").distinct()
val cropListDF = agriculTrendsDF.select("Crop").distinct()

agriculTrendsDF.write.mode(SaveMode.Overwrite).saveAsTable("xl2700.AgriculTrendsAggregation")
countryListDF.write.mode(SaveMode.Overwrite).saveAsTable("xl2700.AgriculTrendsCountries")
cropListDF.write.mode(SaveMode.Overwrite).saveAsTable("xl2700.AgriculTrendsCrops")
