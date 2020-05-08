import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SaveMode
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object BDAD_project {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("regression").getOrCreate()
    import spark.implicits._
    def BDAD_regression(): Unit = {
      val countryCodeDF = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:/user/xl2700/agricultrends/FAOCountryCode.csv").withColumnRenamed("Short name", "Country").select("Country", "ISO3", "FAOSTAT")
      val precipitationDF = spark.read.option("inferSchema", "true").csv("hdfs:/user/xl2700/agricultrends/climate/precipitation_etl/").toDF("ISO3", "Year", "Precipitation")
      val temperatureDF = spark.read.option("inferSchema", "true").csv("hdfs:/user/xl2700/agricultrends/climate/temperature_etl/").toDF("ISO3", "Year", "Temperature")
      val productionDF = spark.read.option("inferSchema", "true").csv("hdfs:/user/yl6183/BDAD_project/cleaned_data_v2").toDF("CountryCode", "Country", "CropCode", "Crop", "Year", "AreaHarvested", "Yield", "Production")
      val producerPriceDF = spark.read.option("inferSchema", "true").csv("hdfs:/user/iil209/bdad_project/cleaned_data_combined_v2").toDF("CountryCode", "Country", "CropCode", "Crop", "Year", "ProducerPrice")

      val weatherDF = precipitationDF.join(temperatureDF, Seq("ISO3", "Year"), "outer")
      val cropDF = productionDF.join(producerPriceDF.select("CountryCode", "CropCode", "Year", "ProducerPrice"), Seq("CountryCode", "CropCode", "Year"), "left").select("CountryCode", "Crop", "Year", "AreaHarvested", "Yield", "Production", "ProducerPrice")
      val agriculTrendsDF = cropDF.join(countryCodeDF, cropDF("CountryCode") === countryCodeDF("FAOSTAT"), "left").join(weatherDF, Seq("ISO3", "Year"), "left").select("Country", "Crop", "Year", "AreaHarvested", "Yield", "Production", "ProducerPrice", "Precipitation", "Temperature").na.drop(Seq("Country", "Crop", "Year", "AreaHarvested", "Yield", "Production", "Precipitation", "Temperature"))

      val countryListDF = agriculTrendsDF.select("Country").distinct()
      val cropListDF = agriculTrendsDF.select("Crop").distinct()

      agriculTrendsDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsAggregation")
      countryListDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsCountries")
      cropListDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsCrops")
    }

    def iter_price_yield(): Unit = {
      val agriculTrendsDF = spark.sql("SELECT * FROM yl6183.AgriculTrendsAggregation").na.drop()

      // Correlation
      val priceCorrelationDF = agriculTrendsDF.groupBy("Country", "Crop").agg(corr("ProducerPrice", "Production").alias("ProducerPriceProductionCorr")).withColumn("AbsProducerPriceProductionCorr", abs(col("ProducerPriceProductionCorr"))).na.drop()
      //priceCorrelationDF.agg(avg("AbsProducerPriceProductionCorr"), min("ProducerPriceProductionCorr"), max("ProducerPriceProductionCorr")).show()
      val agriculTrendsLastYearDF = agriculTrendsDF.withColumnRenamed("Year", "LastYear").withColumnRenamed("Production", "LastYearProduction").withColumnRenamed("ProducerPrice", "LastYearProducerPrice").select("Country", "Crop", "LastYear", "LastYearProduction", "LastYearProducerPrice")
      // Correlation
      val agriculTrendsChangeRateDF = agriculTrendsDF.withColumn("LastYear", col("Year") - 1).join(agriculTrendsLastYearDF, Seq("Country", "Crop", "LastYear"), "inner").withColumn("ProductionChangeRate", (col("Production") - col("LastYearProduction")) / col("LastYearProduction")).withColumn("ProducerPriceChangeRate", (col("ProducerPrice") - col("LastYearProducerPrice")) / col("LastYearProducerPrice")).select("Country", "Crop", "Year", "Production", "ProductionChangeRate", "ProducerPrice", "ProducerPriceChangeRate")
      agriculTrendsChangeRateDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsChangeRate")
      //agriculTrendsChangeRateDF.count()

      val priceChangeCorrelationDF = agriculTrendsChangeRateDF.groupBy("Country", "Crop").agg(corr("ProducerPriceChangeRate", "ProductionChangeRate").alias("ProducerPriceProductionChangeRateCorr")).withColumn("AbsProducerPriceProductionChangeRateCorr", abs(col("ProducerPriceProductionChangeRateCorr"))).na.drop()
      //priceChangeCorrelationDF.agg(avg("AbsProducerPriceProductionChangeRateCorr"), min("ProducerPriceProductionChangeRateCorr"), max("ProducerPriceProductionChangeRateCorr")).show()
      val abnormalChangeRateDF = agriculTrendsChangeRateDF.select("Country", "Crop", "Year", "ProducerPriceChangeRate").na.drop().withColumn("ChangeRateLow", when(col("ProducerPriceChangeRate") < -0.8, true).otherwise(false)).withColumn("ChangeRateHigh", when(col("ProducerPriceChangeRate") > 1, true).otherwise(false))

      val abnormalCountryYearCountDF = abnormalChangeRateDF.groupBy("Country", "Year").agg(count("Crop").alias("Count")).join(abnormalChangeRateDF.filter(col("ChangeRateLow") === true).groupBy("Country", "Year").agg(count("ChangeRateLow").alias("ChangeRateLowCount")), Seq("Country", "Year"), "left").join(abnormalChangeRateDF.filter(col("ChangeRateHigh") === true).groupBy("Country", "Year").agg(count("ChangeRateHigh").alias("ChangeRateHighCount")), Seq("Country", "Year"), "left").na.fill(0)
      val normalCountryYearDF = abnormalCountryYearCountDF.filter(col("ChangeRateLowCount") / col("Count") < 0.9 && col("ChangeRateHighCount") / col("Count") < 0.9).select("Country", "Year")
      val agriculTrendsChangeRateFixedDF = normalCountryYearDF.join(agriculTrendsChangeRateDF, Seq("Country", "Year"), "leftouter").na.drop()
      agriculTrendsChangeRateFixedDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsChangeRateFixed")

      val priceChangeCorrelationFixedDF = agriculTrendsChangeRateFixedDF.groupBy("Country", "Crop").agg(corr("ProducerPriceChangeRate", "ProductionChangeRate").alias("ProducerPriceProductionChangeRateCorr")).withColumn("AbsProducerPriceProductionChangeRateCorr", abs(col("ProducerPriceProductionChangeRateCorr"))).na.drop()

      val lowerProducerPriceCount = agriculTrendsChangeRateFixedDF.filter(col("ProducerPriceChangeRate") < -0.5).count()

      val lowerProducerPriceHigherProductionCount = agriculTrendsChangeRateFixedDF.filter(col("ProducerPriceChangeRate") < -0.5 && col("ProductionChangeRate") > 0).count()

      val lowerProducerPriceHigherProductionRate = lowerProducerPriceHigherProductionCount.toFloat / lowerProducerPriceCount.toFloat
    }

    def iter_yield_country(): Unit = {
      val agriculTrendsDF = spark.sql("SELECT * FROM yl6183.AgriculTrendsAggregation")

      // Group by Country and Crop and Aggregate Sum of Yield
      val totalYieldDF = agriculTrendsDF.groupBy("country", "crop").agg(sum("yield") as "totalYield")
      val windowSpecYield  = Window.partitionBy("country").orderBy(col("totalYield").desc)

      // Query for showing the crop that has the most yield in each country
      // val mostYieldDF = totalYieldDF.withColumn("rank",dense_rank().over(windowSpecYield)).select("country", "crop", "totalYield").where("rank ==1")
      val mostYieldDF = totalYieldDF.withColumn("row_num",row_number.over(windowSpecYield)).select("country", "crop", "totalYield").where("row_num == 1")
      mostYieldDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsMostYield")

      // Query for top 10 crop yields for each country
      val top10YieldDF = totalYieldDF.withColumn("row_num",row_number.over(windowSpecYield)).select("country", "crop", "totalYield").where("row_num <= 10")
      top10YieldDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsTop10Yield")

      // Group by Country and Crop and Aggregate Sum of Production
      val totalProductionDF = agriculTrendsDF.groupBy("country", "crop").agg(sum("production") as "totalProduction")
      val windowSpecProd  = Window.partitionBy("country").orderBy(col("totalProduction").desc)

      // Query for showing the crop that has the most production in each country
      // val mostProdDF = totalProductionDF.withColumn("rank",dense_rank().over(windowSpecProd)).select("country", "crop", "totalProduction").where("rank ==1")
      val mostProdDF = totalProductionDF.withColumn("row_num",row_number.over(windowSpecProd)).select("country", "crop", "totalProduction").where("row_num == 1")

      mostProdDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsMostProd")

      // Query for showing the top 10 crops that have the most production in each country
      val top10ProdDF =totalProductionDF.withColumn("row_num",row_number.over(windowSpecProd)).select("country", "crop", "totalProduction").where("row_num <= 10")
      top10ProdDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsTop10Prod")

      // Query for total crop yield in each country
      val countryTotalYieldDF = agriculTrendsDF.groupBy("country").agg(sum("yield") as "totalYield").orderBy(col("totalYield").desc)
      countryTotalYieldDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsTotalYield")

      // Query for total crop production quantity in each country
      val countryTotalProdDF = agriculTrendsDF.groupBy("country").agg(sum("production") as "totalProduction").orderBy(col("totalProduction").desc)
      countryTotalProdDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsTotalProd")
    }

    def iter_yield_weather(): Unit = {
      val agriculTrendsDF = spark.sql("SELECT * FROM yl6183.AgriculTrendsAggregation")

      // Correlation

      val yieldCorrelationDF = agriculTrendsDF.groupBy("Crop").agg(corr("Yield", "Precipitation").alias("YieldPrecipitationCorr")).join(agriculTrendsDF.groupBy("Crop").agg(corr("Yield", "Temperature").alias("YieldTemperatureCorr")), Seq("Crop"), "inner").withColumn("AbsYieldPrecipitationCorr", abs(col("YieldPrecipitationCorr"))).withColumn("AbsYieldTemperatureCorr", abs(col("YieldTemperatureCorr"))).withColumn("MaxAbsYieldCorr", greatest(col("AbsYieldPrecipitationCorr"), col("AbsYieldTemperatureCorr")))

      yieldCorrelationDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsYieldCorrelation")

      //yieldCorrelationDF.agg(avg("AbsYieldPrecipitationCorr"), avg("AbsYieldTemperatureCorr"), avg("MaxAbsYieldCorr"), max("AbsYieldPrecipitationCorr"), max("AbsYieldTemperatureCorr")).show()
      /*
       * +------------------------------+----------------------------+--------------------+------------------------------+----------------------------+
       * |avg(AbsYieldPrecipitationCorr)|avg(AbsYieldTemperatureCorr)|avg(MaxAbsYieldCorr)|max(AbsYieldPrecipitationCorr)|max(AbsYieldTemperatureCorr)|
       * +------------------------------+----------------------------+--------------------+------------------------------+----------------------------+
       * |           0.15804906228317994|         0.21716067188378688| 0.26076262461768784|            0.8571434541498717|          0.8448481150581663|
       * +------------------------------+----------------------------+--------------------+------------------------------+----------------------------+
       */

      // Round and group, for Yield vs. Precipitation & Temperature by Crop heat map

      val yieldHeatMapDF = agriculTrendsDF.withColumn("RoundPrecipitation", floor(col("Precipitation") / 10) * 10).withColumn("RoundTemperature", floor(col("Temperature"))).groupBy("Crop", "RoundPrecipitation", "RoundTemperature").agg(sum("Yield").alias("YieldSum"), count(lit(1)).alias("YieldRecordNum"), avg("Yield").alias("YieldAvg"))

      yieldHeatMapDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsYieldHeatMap")

      // Get all distinct weather data

      val agriculTrendsWeatherDF = agriculTrendsDF.select("Country", "Year", "Precipitation", "Temperature").distinct()

      agriculTrendsWeatherDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsWeather")
    }

    def regression_price(): Unit = {
      val agriculTrendsChangeRateDF = spark.sql("SELECT * FROM yl6183.AgriculTrendsChangeRateFixed").select("Country", "Crop", "ProducerPriceChangeRate", "ProductionChangeRate").na.drop()

      // Prepare training data by adding sequential index to each unique (country, crop) pair
      val groupDF = agriculTrendsChangeRateDF.select("Country", "Crop").distinct().rdd.zipWithIndex().map(record => (record._1.getString(0), record._1.getString(1), record._2.toInt)).toDF("Country", "Crop", "GroupId")
      val groupCount = groupDF.count().toInt
      val trainingDF = agriculTrendsChangeRateDF.join(groupDF, Seq("Country", "Crop"), "left").select("GroupId", "ProducerPriceChangeRate", "ProductionChangeRate")

      // Linear regression, intercept fitting set to false by default
      val trainingRDD = trainingDF.rdd.map(record => LabeledPoint(record.getDouble(1), Vectors.sparse(groupCount * 2, Array(record.getInt(0), record.getInt(0) + groupCount), Array(record.getDouble(2), 1.0))))
      val model = LinearRegressionWithSGD.train(trainingRDD, 100, 1)
      // val trainingDataDS = spark.createDataset(trainingDataRDD)
      // val parameter = new LinearRegression().setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8).setFitIntercept(false)
      // val model = parameter.fit(trainingDataDS)

      // Prepare data for prediction test
      val testRDD = trainingDF.rdd.map(record => (record.getInt(0), LabeledPoint(record.getDouble(1), Vectors.sparse(groupCount * 2, Array(record.getInt(0), record.getInt(0) + groupCount), Array(record.getDouble(2), 1.0)))))
      val countRDD = testRDD.map(record => (record._1, 1)).reduceByKey((a, b) => a + b)

      // Calculate MSE
      val predRDD = testRDD.map{ record => val predict = model.predict(record._2.features)
        (record._1, (record._2.label - predict) * (record._2.label - predict))
      }
      val MSEDF = predRDD.reduceByKey((a, b) => a + b).join(countRDD).map(record => (record._1, record._2._1 / record._2._2)).toDF("GroupId", "MSE")
      val weights = model.weights.toArray.toList.splitAt(groupCount)
      val coefficients = weights._1 zip weights._2
      val coefficientsDF = spark.sparkContext.parallelize(coefficients).zipWithIndex().map(record => (record._1._1, record._1._2, record._2)).toDF("Coefficient", "Intercept", "GroupId")
      val priceChangeRegressionDF = groupDF.join(coefficientsDF, Seq("GroupId"), "left").select("Country", "Crop", "Coefficient", "Intercept")

      priceChangeRegressionDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsPriceChangeRegression")
    }

    def regression_yield(): Unit = {
      val agriYieldDF = spark.sql("SELECT * FROM yl6183.AgriculTrendsAggregation").select("Country","Crop","Yield","Precipitation","Temperature")
      val distDF = agriYieldDF.select("Country","Crop").distinct()	// collect distinct (country,crop) pair
      val distRDD = distDF.rdd.zipWithIndex().map(row => (row._1.getString(0),row._1.getString(1),row._2.toInt)).toDF("Country","Crop","ID")	// zip (country,crop) pair with ID => (country, crop, ID)
      val trainDF = agriYieldDF.join(distRDD,Seq("Country","Crop"),"left").select("ID","Yield","Precipitation","Temperature")	// join with agriYieldDF
      // (country, crop, yield, predipitation, temperature, ID)

      // setting training data for linear regression
      // LabeledPoint(yield, sparse_vector(index array, value array))
      val cnt = distDF.count.toInt
      val trainRDD = trainDF.rdd.map(row => LabeledPoint(row.getDouble(1), Vectors.sparse(cnt*3,Array(row.getInt(0),row.getInt(0)+cnt,row.getInt(0)+2*cnt),Array(row.getDouble(2),row.getDouble(3),1.0))))
      val model = LinearRegressionWithSGD.train(trainRDD,10000,0.01)	// train model

      // data for prediction test
      val testRDD = trainDF.rdd.map(row => (row.getInt(0), LabeledPoint(row.getDouble(1), Vectors.sparse(cnt*3,Array(row.getInt(0),row.getInt(0)+cnt,row.getInt(0)+2*cnt),Array(row.getDouble(2),row.getDouble(3),1.0)))))
      val countRDD = testRDD.map(tu => (tu._1,1))
      val reduceRDD = countRDD.reduceByKey((a,b) => a+b)	//count (ID, number of points)

      // MSE
      val labelandpred = testRDD.map{ tu => val predict = model.predict(tu._2.features)
        (tu._1,(tu._2.label-predict)*(tu._2.label-predict))	// (ID, (label - predict)^2)
      }
      val MSEsumRDD = labelandpred.reduceByKey((a,b) => a+b)	// (ID,sum of (label-predict)^2)
      val MSEjoinRDD = MSEsumRDD.join(reduceRDD)		//(ID, (sum, number of points))
      val MSERDD = MSEjoinRDD.map(tu => (tu._1,tu._2._1/tu._2._2))	// (ID, MSE)
      val MSEDF = MSERDD.toDF("ID","MSE")
      val weights = model.weights.toArray.toList.grouped(cnt).toList	// get model weights
      val coe = weights(0) zip weights(1) zip weights(2)	// ((coefficient1, coefficient2), intercept)
      val coeRDD = spark.sparkContext.parallelize(coe)	// list to rdd
      val coefficient = coeRDD.map(tu => (tu._1._1,tu._1._2,tu._2))	// ((coefficient1, coefficient2), intercept) => (coefficient1, coefficient2, intercept)
      val coeDF = coefficient.zipWithIndex().map(tu => (tu._1._1,tu._1._2,tu._1._3,tu._2)).toDF("Coefficient1","Coefficient2","Intercept","ID")	// zip with ID
      val regcoeDF = coeDF.join(MSEDF, Seq("ID"),"left")	// join weights with MSE
      val regressionDF = distRDD.join(regcoeDF,  Seq("ID"),"left").select("Country","Crop","Coefficient1","Coefficient2","Intercept","MSE")	// join with (country, crop) pair with the same ID
      regressionDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsYieldRegression")
    }
    //val sparkConf = new SparkConf().setAppName("regression")
    //val sc = new SparkContext(sparkConf)

    BDAD_regression()
    iter_price_yield()
    iter_yield_country()
    iter_yield_weather()
    regression_price()
    regression_yield()
  }


}
//https://spark.apache.org/docs/latest/quick-start.html

// TO DO
// 1. Find better parameters if possible
// 2. Loop over (country, crop) pairs
// 3. Store all (country, crop) pairs and their coefficients & intercept to a table
