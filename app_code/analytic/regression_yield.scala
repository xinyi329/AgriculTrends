import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

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
MSEDF.agg(avg("MSE"), min("MSE"), max("MSE")).show()
//+--------------------+--------+--------------------+                            
//|            avg(MSE)|min(MSE)|            max(MSE)|
//+--------------------+--------+--------------------+
//|9.634004795811632E11|     0.0|6.608717495110106E15|
//+--------------------+--------+--------------------+

val sMSEDF = MSEDF.filter("MSE<1E11")
sMSEDF.agg(avg("MSE"),min("MSE"),max("MSE")).show()
//+--------------------+-------------------+                                      
//|            max(MSE)|           avg(MSE)|
//+--------------------+-------------------+
//|9.983001535610995E10|6.729535200693268E9|
//+--------------------+-------------------+


val weights = model.weights.toArray.toList.grouped(cnt).toList	// get model weights
val coe = weights(0) zip weights(1) zip weights(2)	// ((coefficient1, coefficient2), intercept)
val coeRDD = sc.parallelize(coe)	// list to rdd
val coefficient = coeRDD.map(tu => (tu._1._1,tu._1._2,tu._2))	// ((coefficient1, coefficient2), intercept) => (coefficient1, coefficient2, intercept)
val coeDF = coefficient.zipWithIndex().map(tu => (tu._1._1,tu._1._2,tu._1._3,tu._2)).toDF("Coefficient1","Coefficient2","Intercept","ID")	// zip with ID
val regcoeDF = coeDF.join(MSEDF, Seq("ID"),"left")	// join weights with MSE
val regressionDF = distRDD.join(regcoeDF,  Seq("ID"),"left").select("Country","Crop","Coefficient1","Coefficient2","Intercept","MSE")	// join with (country, crop) pair with the same ID
import org.apache.spark.sql.SaveMode
regressionDF.write.mode(SaveMode.Overwrite).saveAsTable("yl6183.AgriculTrendsYieldRegression")	// save table
