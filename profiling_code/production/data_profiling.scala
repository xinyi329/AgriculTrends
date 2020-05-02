/*
this part uses some RDDs from the cleaning steps to explore data
*/
//deleting useless rows
myRDD.count //51155
usefulRDD.count	//50099 with one header row


//profiling: calculating the sum and average of each part
areaRDD.count	//16418
val areaPro1 = areaRDDc5.map(inA => inA.sum)	//sum of each array
val areaPro2 = areaPro1.reduce((v1,v2) => v1 + v2)	//sum of all arrays = 1.013783286379E12
val areaPro3 = areaPro2/areaRDD.count	//average of all arrays = 6.1748281543367036E7

yieldRDD.count	//16375
val yieldPro1 = yieldRDDc5.map(inA => inA.sum)
val yieldPro2 = yieldPro1.reduce((v1,v2) => v1 + v2)	//8.5594722978E10
val yieldPro3 = yieldPro2/yieldRDD.count	//5227158.655145038

prodRDD.count	//17305
val prodPro1 = prodRDDc5.map(inA => inA.sum)
val prodPro2 = prodPro1.reduce((v1,v2) => v1 + v2)	//3.297570111421E12
val prodPro3 = prodPro2/prodRDD.count	//1.9055591513556775E8