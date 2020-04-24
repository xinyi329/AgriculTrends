// Executed through REPL

def parseCsvFile(code: String, csvDataArray: Array[String]): Array[(String, Int, Double)] = {
    val data = csvDataArray.drop(1)
    val dataIterator = data.iterator
    val cleanedDataIterator = dataIterator.map(dataString => (code, dataString.split(",")(0).toInt, dataString.split(",")(1).toDouble))
    cleanedDataIterator.toArray
}

/* Precipitation */

val precipitationRDD = sc.wholeTextFiles("hdfs:/user/xl2700/agricultrends/climate/precipitation")
val splitedPrecipitationRDD = precipitationRDD.map(record => (record._1, record._2.split("\n")))
val cleanedPrecipitationRDD = splitedPrecipitationRDD.filter(record => record._2.size > 0).map(record => parseCsvFile(record._1.substring(61, 64), record._2)).flatMap(record => record)
val cleanedPrecipitationOutputRDD = cleanedPrecipitationRDD.map(record => record.toString).map(line => line.substring(1, line.length() - 1))
cleanedPrecipitationOutputRDD.saveAsTextFile("hdfs:/user/xl2700/agricultrends/climate/precipitation_etl")

/* Temperature */

val temperatureRDD = sc.wholeTextFiles("hdfs:/user/xl2700/agricultrends/climate/temperature")
val splitedTemperatureRDD = temperatureRDD.map(record => (record._1, record._2.split("\n")))
val cleanedTemperatureRDD = splitedTemperatureRDD.filter(record => record._2.size > 0).map(record => parseCsvFile(record._1.substring(59, 62), record._2)).flatMap(record => record)
val cleanedTemperatureOutputRDD = cleanedTemperatureRDD.map(record => record.toString).map(line => line.substring(1, line.length() - 1))
cleanedTemperatureOutputRDD.saveAsTextFile("hdfs:/user/xl2700/agricultrends/climate/temperature_etl")

// This dataset will be linked to other datasets by ISO3 Codes listed in FAO Country Codes:
// http://www.fao.org/countryprofiles/iso3list/en/
