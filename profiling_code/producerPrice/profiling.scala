// Ian I Lam (iil209)


// original datasets

// In the cleaning step, two datasets from two consecutive time ranges are cleaned and combined
// to form one big dataset.

// first original dataset: years 1991 - 2018
val datafile:String = "hdfs:/user/iil209/bdad_project/Prices_E_All_Data_NOFLAG.csv"
val data = sc.textFile(datafile)

val dropped_header = data.mapPartitionsWithIndex {
  (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}
dropped_header.count()
// number of records in the first original dataset
// res22: Long = 26797


// second original dataset: years 1966 - 1990
val data_archive_file:String = "hdfs:/user/iil209/bdad_project/PricesArchive_E_All_Data.csv"
val data_archive = sc.textFile(data_archive_file)

val dropped_header_archive = data_archive.mapPartitionsWithIndex {
  (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}
dropped_header_archive.count()
// number of records in the second original dataset
// res23: Long = 139738

// new dataset after cleaning, formatting, and transformation
val new_datafiles:String = "hdfs:/user/iil209/bdad_project/cleaned_data_combined/part-*"
val new_data = sc.textFile(new_datafiles)
new_data.count()
// number of records of new dataset
// res24: Long = 11578
 

// scala> cleaned_data_combined.count()
// res8: Long = 292147    
