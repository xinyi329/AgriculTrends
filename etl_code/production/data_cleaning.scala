al cropfile = "BDAD_project/Production_Crops_E_All_Data_Normalized.csv"
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
finalRDD.saveAsTextFile("BDAD_project/cleaned_data_v2")
//result example: 201,Somalia,328,Seed cotton,1964,10000.000000, 3290.000000, 3290.000000
