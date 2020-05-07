import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.SaveMode
//import spark.implicits._
import org.apache.spark.sql.DataFrame

case class RegResult(Country: String, Crop: String, Coefficients1: Double, Coefficients2: Double, Intercept: Double)

def linearReg(countryname: String, cropname: String, yieldDataFrame: DataFrame): DataFrame = {
	val yieldDF = spark.sql("SELECT Yield, Precipitation, Temperature FROM yl6183.AgriculTrendsAggregation WHERE Country = '%s' AND Crop = '%s'".format(countryname, cropname))
	var newDataFrame = yieldDataFrame
	if (!yieldDF.head(1).isEmpty){
		val yieldRDD = yieldDF.rdd.map(record => LabeledPoint(record.getDouble(0),Vectors.dense(record.getDouble(1),record.getDouble(2))))
		//val yieldRDD = yieldDF.rdd.map(record => LabeledPoint(record(0), Vectors.dense(record(1),record(2))))
		val yieldDS = spark.createDataset(yieldRDD)
		val parameter = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
		val yieldModel = parameter.fit(yieldDS)
		println(s"Coefficients: ${yieldModel.coefficients} Intercept: ${yieldModel.intercept}")
		//val trainingSummary = yieldModel.summary
		//println(s"numIterations: ${trainingSummary.totalIterations}")
		//println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
		//trainingSummary.residuals.show()
		//println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
		//println(s"r2: ${trainingSummary.r2}")
		val addSeq = Seq((countryname, cropname, yieldModel.coefficients(0),yieldModel.coefficients(1),yieldModel.intercept)).toDF("Country","Crop","Coefficients1","Coefficients2","Intercept")
		newDataFrame = yieldDataFrame.union(addSeq)
	}
	newDataFrame
}

// Load data
val countryDF = spark.sql("SELECT * FROM yl6183.AgriculTrendsCountries")
val cropDF = spark.sql("SELECT * FROM yl6183.AgriculTrendsCrops")

var resultDF = spark.emptyDataset[RegResult].toDF

for (countryrow <- countryDF.rdd.collect){
	for (croprow <- cropDF.rdd.collect){
		val country = countryrow.mkString("").replaceAll("\'s","""\\\'s""")
		val crop = croprow.mkString("").replaceAll("\'s","""\\\'s""")
		resultDF = linearReg(country,crop,resultDF)
	}
}

resultDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsRegression")


// TO DO
// 1. Find better parameters if possible
// 2. Loop over (country, crop) pairs
// 3. Store all (country, crop) pairs and their coefficients & intercept to a table
