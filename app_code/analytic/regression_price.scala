// Reference: https://stackoverflow.com/questions/33999156/grouped-linear-regression-in-spark

// import org.apache.spark.ml.feature.LabeledPoint
// import org.apache.spark.ml.linalg.Vectors
// import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.sql.SaveMode

val agriculTrendsChangeRateDF = spark.sql("SELECT * FROM xl2700.AgriculTrendsChangeRateFixed")
                                     .select("Country", "Crop", "ProducerPriceChangeRate", "ProductionChangeRate")
                                     .na.drop()

// Prepare training data by adding sequential index to each unique (country, crop) pair
val groupDF = agriculTrendsChangeRateDF.select("Country", "Crop").distinct()
                                       .rdd.zipWithIndex().map(record => (record._1.getString(0), record._1.getString(1), record._2.toInt))
                                       .toDF("Country", "Crop", "GroupId")
val groupCount = groupDF.count().toInt
val trainingDF = agriculTrendsChangeRateDF.join(groupDF, Seq("Country", "Crop"), "left")
                                          .select("GroupId", "ProducerPriceChangeRate", "ProductionChangeRate")

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
MSEDF.describe("MSE").show()
/*
 * +-------+------------------+                                                    
 * |summary|               MSE|
 * +-------+------------------+
 * |  count|              6116|
 * |   mean| 0.423553838183119|
 * | stddev| 4.212705080774702|
 * |    min|               0.0|
 * |    max|215.66880309546494|
 * +-------+------------------+
 */

// Create table for coefficients and intercepts regarding country and crop
val weights = model.weights.toArray.toList.splitAt(groupCount)
val coefficients = weights._1 zip weights._2
val coefficientsDF = sc.parallelize(coefficients).zipWithIndex().map(record => (record._1._1, record._1._2, record._2)).toDF("Coefficient", "Intercept", "GroupId")
val priceChangeRegressionDF = groupDF.join(coefficientsDF, Seq("GroupId"), "left").select("Country", "Crop", "Coefficient", "Intercept")

priceChangeRegressionDF.write.mode(SaveMode.Overwrite).saveAsTable("xl2700.AgriculTrendsPriceChangeRegression")
