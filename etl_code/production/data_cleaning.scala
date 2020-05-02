/*
Data are divided into three parts: area harvested, yield, and producton.
Dividing the data can make it easier for us to do further analytics on the data
three parts have similar cleaning procedure, but with different details
the final parts are saved too BDAD_project in different directories
*/

import scala.collection.mutable.ArrayBuffer

def getnum(arr: Array[String]): Array[Double] = {
	var buf = ArrayBuffer[Double]()
	var a = ""
	for (a <- arr){
		if (a == ""){
			buf += 0}
		else {
			buf += a.toDouble}
		}
	buf.toArray
}

val agrifile = "BDAD_project/Production_Crops_E_All_Data_NOFLAG.csv"
val myRDD = sc.textFile(agrifile)	//all rows
val usefulRDD = myRDD.filter(line => !(line.contains(",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,")))	//remove empty data
//usefulRDD.count => 50099

/****************/
/*area harvested*/
/****************/
//cleaning
//areaRDD.count => 16418
val areaRDD = usefulRDD.filter(line => line.contains("Area harvested"))		
val areaRDDc1 = areaRDD.map(line => line.substring(line.indexOf(",")+1,line.length))
val areaRDDc2 = areaRDDc1.map(line => (line.split("\",\"")(0),line.split("\",\"")(2)))
val areaRDDc22 = areaRDDc2.map(pair => (pair._1.substring(1,pair._1.length),pair._2.replaceAll(",","")))
val areaRDDc3 = areaRDDc1.map(line => line.substring(line.indexOf("ha\",")+4,line.length))
val areaRDDc4 = areaRDDc3.map(line => line.replaceAll("\"","").split(","))
val areaRDDc5 = areaRDDc4.map(inA => getnum(inA))
val areaRDDc6 = areaRDDc5.map(inA => inA.mkString(","))
val areaRDDc7 = areaRDDc22.zip(areaRDDc6)
val areaRDDc8 = areaRDDc7.map(tu => tu.toString.replaceAll("\\(","").replaceAll("\\)",""))
areaRDDc8.saveAsTextFile("BDAD_project/areaharvested")
//result example: Afghanistan,Almonds with shell,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,5900.0,6000.0,6000.0,6000.0,5800.0,5800.0,5800.0,5700.0,5700.0,5600.0,5500.0,5500.0,5400.0,5400.0,6037.0,5500.0,5500.0,5500.0,5500.0,5500.0,5500.0,5500.0,5500.0,5500.0,7000.0,9000.0,5500.0,5700.0,12000.0,11768.0,12000.0,12000.0,12000.0,11029.0,11210.0,13469.0,13490.0,14114.0,13703.0,14676.0,19481.0,19793.0,20053.0

/*****************/
/******yield******/
/*****************/
//yieldRDD.count => 16375
val yieldRDD = usefulRDD.filter(line => line.contains("Yield"))
val yieldRDDc1 = yieldRDD.map(line => line.substring(line.indexOf(",")+1,line.length))
val yieldRDDc2 = yieldRDDc1.map(line => (line.split("\",\"")(0),line.split("\",\"")(2)))
val yieldRDDc22 = yieldRDDc2.map(pair => (pair._1.substring(1,pair._1.length),pair._2.replaceAll(",","")))
val yieldRDDc3 = yieldRDDc1.map(line => line.substring(line.indexOf("ha\",")+4,line.length))
val yieldRDDc4 = yieldRDDc3.map(line => line.replaceAll("\"","").split(","))
val yieldRDDc5 = yieldRDDc4.map(inA => getnum(inA))
val yieldRDDc6 = yieldRDDc5.map(inA => inA.mkString(","))
val yieldRDDc7 = yieldRDDc22.zip(yieldRDDc6)
val yieldRDDc8 = yieldRDDc7.map(tu => tu.toString.replaceAll("\\(","").replaceAll("\\)",""))
yieldRDDc8.saveAsTextFile("BDAD_project/yield")

/*****************/
/***Production****/
/*****************/
//prodRDD.count = 17305
val prodRDD = usefulRDD.filter(line => line.contains("Production"))
val prodRDDc1 = prodRDD.map(line => line.substring(line.indexOf(",")+1,line.length))
val prodRDDc2 = prodRDDc1.map(line => (line.split("\",\"")(0),line.split("\",\"")(2)))
val prodRDDc22 = prodRDDc2.map(pair => (pair._1.substring(1,pair._1.length),pair._2.replaceAll(",","")))
val prodRDDc3 = prodRDDc1.map(line => line.substring(line.indexOf("tonnes\",")+8,line.length))
val prodRDDc4 = prodRDDc3.map(line => line.replaceAll("\"","").split(","))
val prodRDDc5 = prodRDDc4.map(inA => getnum(inA))
val prodRDDc6 = prodRDDc5.map(inA => inA.mkString(","))
val prodRDDc7 = prodRDDc22.zip(prodRDDc6)
val prodRDDc8 = prodRDDc7.map(tu => tu.toString.replaceAll("\\(","").replaceAll("\\)",""))
prodRDDc8.saveAsTextFile("BDAD_project/production")



val cropfile = "BDAD_project/Production_Crops_E_All_Data_Normalized.csv"
val myRDD = sc.textFile(cropfile)
val myRDD2 = myRDD.filter(line => !line.contains("Flag"))	//remove header
val splitRDD = myRDD.map(line => line.split("\",\""))
val filterRDD = splitRDD.filter(inA => inA.size == 11)
val partRDD = filterRDD.map(inA => ((inA(0).substring(1,inA(0).length),inA(1),inA(2),inA(3),inA(7)),(inA(4),inA(9))))
val groupRDD = partRDD.groupByKey()
val stringRDD = groupRDD.map({case((x1,x2,x3,x4,x5),b) => ((x1,x2.replaceAll(",",""),x3,x4.replaceAll(",",""),x5),b.toString.substring(13,b.toString.length))})
val nonemptyRDD = stringRDD.filter({case(a,b) => b.contains("5312,")&&b.contains("5419,")&&b.contains("5510,")})
val pairRDD = nonemptyRDD.map(pair => pair.toString)
val finalRDD = pairRDD.map(line => line.replaceAll("\\(","").replaceAll("\\)","").replaceAll("5312,","").replaceAll("5419,","").replaceAll("5510,",""))
finalRDD.saveAsTextFile("BDAD_project/cleaned_data")
//result example: 201,Somalia,328,Seed cotton,1964,10000.000000, 3290.000000, 3290.000000

