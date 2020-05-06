# AgriculTrends: Big Data Analytics for Agriculture

Project for Big Data Application Development @ NYU  
Team Members: Xinyi Liu (xl2700), Yiming Li (yl6183), Ian Lam (iil209)

## Directories Description

```
.
├── README.md
├── app_code
│   ├── analytic
│   │   ├── aggregation.scala
│   │   ├── iteration_price_yield.scala
│   │   ├── iteration_yield_country.scala
│   │   ├── iteration_yield_weather.scala
│   │   ├── regression.scala
│   │   ├── regression_price.scala
│   │   └── regression_yield.scala
│   └── visualization
│       ├── AgriculTrends.twb
│       └── tables
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
* /app_code: source code for the application, includes the Spark Scala analytics code and the Tableau visualization code
* /data_ingest: commands used to upload each of the three datasets to the Dumbo HDFS
* /etl_code: Scala source code used to clean and transform each of the three datasets in Spark
* /profiling_code: Scala source code for profiling the three datasets, before and after the ETL step
* /screenshots: screenshots of analytic running, includes the analytic result from Spark and the visualization result from Tableau

## How to Build Code

## How to Run Code

## Where to Find Results of Run
All the results of the run will be saved as Hive Tables in the HDFS.

## Input Datasets
### Crops
Source:
Crop data from 1961-2018: http://www.fao.org/faostat/en/#data/QC

Dumbo Location:  
hdfs:/user/yl6183/BDAD_project/cleaned_data_v2
### Producer Price - Annual
Source:  
Producer Price from 1966-1991: www.fao.org/faostat/en/#data/PA  
Producer Price from 1992-2018: www.fao.org/faostat/en/#data/PP

Dumbo Location:  
hdfs:/user/iil209/bdad_project/cleaned_data_combined_v2

### World Bank Climate Data
