val logfile = "BDAD_project/cleaned_data_v2/*"
val dataRDD = sc.textFile(logfile)

val splitRDD = dataRDD.map(line => line.split(","))

val countrycode = splitRDD.map(inA => inA(0).toInt)		// collect countrycode only
val maxcountry = countrycode.reduce((a,b) => {if (a > b) a else b})	//maxcountry: Int = 5817                                                          
val mincountry = countrycode.reduce((a,b) => {if (a < b) a else b})	//mincountry: Int = 1 

val cropcode = splitRDD.map(inA => inA(2).toInt)		// collect cropcode only
val maxcrop = cropcode.reduce((a,b) => {if (a > b) a else b})	//maxcrop: Int = 1841                                                             
val mincrop = cropcode.reduce((a,b) => {if (a < b) a else b})	//mincrop: Int = 15

val areaRDD = splitRDD.map(inA => inA(5).toDouble)		// collect area harvested only
val maxarea = areaRDD.reduce((a,b) => {if (a > b) a else b})	//maxarea: Double = 7.3431974E8                                                   
val minarea = areaRDD.reduce((a,b) => {if (a < b) a else b})	//minarea: Double = 0.0
val areasum = areaRDD.reduce((a,b) => a+b)	//areasum: Double = 1.0137863694E12                                               
val areaAvg = areasum/areaRDD.count 	//areaAvg: Double = 1319982.773329167

val yieldRDD = splitRDD.map(inA => inA(6).toDouble)		// collect yield only
val maxyield = yieldRDD.reduce((a,b) => {if (a > b) a else b})	//maxyield: Double = 1.234728031E9                                                
val minyield = yieldRDD.reduce((a,b) => {if (a < b) a else b})	//minyield: Double = 0.0
val yieldsum = yieldRDD.reduce((a,b) => a+b)	//yieldsum: Double = 8.5595437628E10                                              
val yieldAvg = yieldsum/yieldRDD.count 		//yieldAvg: Double = 111448.03930575629

val prodRDD = splitRDD.map(inA => inA(7).toDouble)		// collect production only
val maxprod = prodRDD.reduce((a,b) => {if (a > b) a else b})	//maxprod: Double = 3.020299421E9                                                 
val minprod = prodRDD.reduce((a,b) => {if (a < b) a else b})	//minprod: Double = 0.0 
val prodsum = prodRDD.reduce((a,b) => a+b)	//prodsum: Double = 3.276273111274E12                                             
val prodAvg = prodsum/prodRDD.count 	//prodAvg: Double = 4265813.980279416                                             

