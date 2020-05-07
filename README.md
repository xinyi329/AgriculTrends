# AgriculTrends: Big Data Analytics for Agriculture

Project for Big Data Application Development @ NYU  
Team Members: Xinyi Liu (xl2700), Yiming Li (yl6183), Ian Lam (iil209)

## Project Description
An analytic is introduced to provide insights in agriculture production and market supply by country. Based on historical climate data, crop production level and producer prices, people can learn the relations among weather, production amount, and producer prices through visualizations. Future harvests and producer prices can be estimated based on the historical data with this analytic. In this way, the supply in the market might be adjusted in advance to benefit both farmers and consumers.

## Directory Structure

```
.
.
├── README.md
├── act_rem_code
├── app_code
│   ├── analytic
│   │   ├── aggregation.scala
│   │   ├── iteration_price_yield.scala
│   │   ├── iteration_yield_country.scala
│   │   ├── iteration_yield_weather.scala
│   │   ├── regression_price.scala
│   │   └── regression_yield.scala
│   └── visualization
│       ├── AgriculTrends.twb
│       └── tables
│           ├── AgriculTrendsAggregation.csv
│           ├── AgriculTrendsChangeRate.csv
│           ├── AgriculTrendsPriceChangeRegression.csv
│           ├── AgriculTrendsWeather.csv
│           ├── AgriculTrendsYieldHeatMap.csv
│           ├── AgriculTrendsYieldRegression.csv
│           ├── agriculTrendsMostYield.csv
│           ├── agriculTrendsTop10Yield.csv
│           └── agriculTrendsTotalYield.csv
├── data_ingest
│   ├── Production
│   │   └── data_ingest.txt
│   ├── climate
│   │   └── climate_ingest.ipynb
│   └── producerPrice
│       └── data_ingest
├── documents
│   ├── AgriculTrends_DesignDiagram.pdf
│   └── AgriculTrends_DesignDiagram.pptx
├── etl_code
│   ├── climate
│   │   └── climate_clean.scala
│   ├── producerPrice
│   │   └── cleaning.scala
│   └── production
│       └── data_cleaning.scala
├── profiling_code
│   ├── climate
│   │   └── climate_profiling.scala
│   ├── producerPrice
│   │   └── profiling.scala
│   └── production
│       └── data_profiling.scala
└── screenshots
    ├── analytic
    └── visualization
```
* `/app_code`: source code for the application, includes the Spark Scala analytics code and the Tableau visualization code
* `/app_code/analytic`:
  * `aggregation.scala`: joins the three datasets into one large dataframe
  * `iteration_price_yield.scala`: analyzes the relationship between producer price and crop yield
  * `iteration_yield_country.scala`: analyzes the relationship between crop yield and the country
  * `iteration_yield_weather.scala`: analyzes the relationship between crop yield and weather
  * `regression_price.scala`: linear regression for producer price
  * `regression_yield.scala`: linear regression for crop yield  
* `/data_ingest`: commands used to upload each of the three datasets to the Dumbo HDFS
* `/etl_code`: Scala source code used to clean and transform each of the three datasets in Spark
* `/profiling_code`: Scala source code for profiling the three datasets, before and after the ETL step
* `/screenshots`: screenshots of analytic running, includes the analytic result from Spark and the visualization result from Tableau

## How to Build/Run Code
No building of code is required for this application.

To upload the datasets into HDFS, follow the commands within `/data_ingest` directory.  

All the cleaning, ETL, and analysis codes are run in the Spark Shell (REPL). In order to run the code, copy and paste the code into the Spark Shell. Comments are also written within the code to provide clearer description of the different analyses.

In order to visualize the results on Tableau, we have to first save our Spark analysis results into Hive Tables in HDFS, then export the Hive tables as csv files to Dumbo locally, and finally manually transfer them to our local computer. This is because the data transfer rate between Tableau and Dumbo directly is too slow to handle the large volume of data.

The following command is used to export a Hive table as csv format:
```
beeline -u jdbc:hive2://babar.es.its.nyu.edu:10000/[netID] -n [netID] -w [textFileContainingUserPassword] --outputformat=csv2 -e "select * from [hiveTableName]" > [output].csv
```


## Where to Find Results of Run
The results of the runs are saved as Hive Tables in the HDFS. The `/screenshots` directory also contains examples of the expected results.


## Input Datasets
### Crops
Source: Crop data from 1961-2018: http://www.fao.org/faostat/en/#data/QC  
Dumbo Location: hdfs:/user/yl6183/BDAD_project/cleaned_data_v2

### Producer Prices - Annual
Source: Producer Prices from 1966-1991: www.fao.org/faostat/en/#data/PA  , Producer Prices from 1992-2018: www.fao.org/faostat/en/#data/PP  
Dumbo Location: hdfs:/user/iil209/bdad_project/cleaned_data_combined_v2

### World Bank Climate Data
Source: https://datahelpdesk.worldbank.org/knowledgebase/articles/902061-climate-data-api  
Dumbo Location: hdfs:/user/xl2700/agricultrends/climate/
