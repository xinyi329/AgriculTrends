// Ian I Lam (iil209)
import scala.collection.mutable.ListBuffer

// Processing
// first dataset: year 1991-2018
val datafile:String = "hdfs:/user/iil209/bdad_project/Prices_E_All_Data_NOFLAG.csv"

val data = sc.textFile(datafile)

// drop the header row
val dropped_header = data.mapPartitionsWithIndex {
  (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}

// replace the commas inside a value with a space
val replaced_commas = dropped_header.map(line => line.replace(", ", " "))

// only keep lines that have "LCU"
val filtered = replaced_commas.filter(line => line.split(",")(6) == "\"LCU\"")

// split one row to several rows with one year for each row
// extract only 6 fields total
// Area Code, Area, Item Code, Item, Year, Producer Price in LCU/tonne
val cleaned_data = filtered.flatMap{line => 
  val dummy_line = line.replace("\"", "") + ",dummy"
  val fields = dummy_line.split(",")
  var records = new ListBuffer[String]()
  var year = 1991
  for (amount <- fields.slice(7,35)) {
    if (amount.length > 0){
      var record = fields(0) + "," + fields(1) + "," + fields(2) + "," + fields(3) + ","
      record = record + year + "," + amount
      records += record
    }
    year += 1
  }
  records.toList
}

// export to text file
cleaned_data.saveAsTextFile("hdfs:/user/iil209/bdad_project/cleaned_data_current_v2")

// Processing
// second dataset: year 1965-1990
val data_archive_file:String = "hdfs:/user/iil209/bdad_project/PricesArchive_E_All_Data.csv"

val data_archive = sc.textFile(data_archive_file)

// drop the header
val dropped_header_archive = data_archive.mapPartitionsWithIndex {
  (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}

// replace the commas within the values with ""
val replaced_commas_archive = dropped_header_archive.map(line => line.replace(", ", " "))

// drop the unneeded columns
// six fields are left: Area Code, Area, Item Code, Item, Producer Price in LCU/tonne
val cleaned_archive_data = replaced_commas_archive.map{line =>
val line_fixed = line.replace("\"", "").replace("m,D", "m D").replace("s,i", "s i")
val fields = line_fixed.replace("\"", "").split(",")
fields(0) + "," + fields(1) + "," + fields(2) + "," + fields(3) + "," + fields(7) + "," + fields(9)
}

// export the dataset
cleaned_archive_data.saveAsTextFile("hdfs:/user/iil209/bdad_project/cleaned_data_archive")


// Combine the two datasets to obtain years 1966 - 2018
val cleaned_data_combined = cleaned_archive_data.union(cleaned_data)

// map-reduce the data
// key is (Area Code, Area, Item Code, Item)
val dict_combined = cleaned_data_combined.map{line =>
val fields = line.split(",")
(fields.slice(0,4).mkString(","), List((fields(4), fields(5))))
}

// reduce the keys and combine the year_produce price
val cleaned_combined = dict_combined.reduceByKey((v1, v2) => v1 ::: v2)


// add the years that were missing
val cleaned_all_years = cleaned_combined.map{line =>
  val year_pairs = line._2
  val arr = Array.fill[String](53)("-1.")
  for (year_pair <- year_pairs){
    println(year_pair._1.toInt)
    arr(year_pair._1.toInt - 1966) = year_pair._2
  }
  line._1 + "," + arr.mkString(",")
}

// export the cleaned combined processed dataset for year 1966 - 2018
cleaned_all_years.saveAsTextFile("hdfs:/user/iil209/bdad_project/cleaned_data_combined")
