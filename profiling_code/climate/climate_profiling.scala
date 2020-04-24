// Executed through REPL
// RDDs are created by codes in Clean-xl2700.scala

/* Precipitation */

precipitationRDD.count()
// Long = 211
cleanedPrecipitationRDD.groupBy(record => record._1).count()
// Long = 209
// Two countries are dropped because no records found.

splitedPrecipitationRDD.map(record => record._2.size).reduce((x, y) => x + y)
// Int = 23544
cleanedPrecipitationRDD.count()
// Long = 23333
// Head lines 211 csv files are removed.

cleanedPrecipitationRDD.map(record => record._2).min()
// Int = 1901
cleanedPrecipitationRDD.map(record => record._2).max()
// Int = 2012
// Year range from 1901 to 2012.

cleanedPrecipitationRDD.map(record => record._2).countByValue().filter(record => record._2 < 209)
// scala.collection.Map[Int,Long] = Map(2010 -> 184, 2011 -> 184, 2012 -> 184)
// Data from 2010 to 2012 are missing in some countries.

cleanedPrecipitationRDD.map(record => record._3).min()
// Double = 1.118749976158142
cleanedPrecipitationRDD.map(record => record._3).max()
// Double = 930.0958251953125
// Precipitation values seem normal.

/* Temperature */

temperatureRDD.count()
// Long = 211
cleanedTemperatureRDD.groupBy(record => record._1).count()
// Long = 209
// Two countries are dropped because no records found.

splitedTemperatureRDD.map(record => record._2.size).reduce((x, y) => x + y)
// Int = 23544
cleanedTemperatureRDD.count()
// Long = 23333
// Head lines 211 csv files are removed.

cleanedTemperatureRDD.map(record => record._2).min()
// Int = 1901
cleanedTemperatureRDD.map(record => record._2).max()
// Int = 2012
// Year range from 1901 to 2012.

cleanedTemperatureRDD.map(record => record._2).countByValue().filter(record => record._2 < 209)
// scala.collection.Map[Int,Long] = Map(2010 -> 184, 2011 -> 184, 2012 -> 184)
// Data from 2010 to 2012 are missing in some countries.

cleanedTemperatureRDD.map(record => record._3).min()
// Double = -18.66438865661621
cleanedTemperatureRDD.map(record => record._3).max()
// Double = 29.658334732055664
// Temperature values seem normal.
