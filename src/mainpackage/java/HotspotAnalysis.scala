/**
  * Created by Nick on 12/5/16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object HotspotAnalysis {
    var data = Array.ofDim[Int](41, 56, 31)
    def mapper(line: String): (Tuple3[Int, Int, Int], Int) = {
        var key = (-1, -1, -1);
        var value = 1;
        try {
            var words = line.trim().split(",")
            var date = words(1).trim().split(" ")(0).split("-")(2).toInt
            var cellnox = 0;
            var cellnoy = 0;
            var cellnox1 = words(6).toDouble
            var cellnoy1 = words(5).toDouble

            var longLimit: Double = -74.25;
            var longUpper: Double = -73.70;
            if ((cellnox1 >= 40.5 && cellnox1 < 40.9) && (cellnoy1 >= longLimit && cellnoy1 < longUpper) && date >= 1 && date <= 31) {
                var cellnox2 = math.floor(cellnox1 * 100);
                var cellnoy2 = math.floor(cellnoy1 * 100) * (-1);
                cellnox = cellnox2.toInt
                cellnoy = cellnoy2.toInt
                date = date - 1;
            }
            else {
                cellnox = -1;
                cellnoy = -1;
                date = -1;
            }
            value = 1;

            key = (date, cellnox, cellnoy);
        } catch {
            case e: Exception =>
        }
        return (key, value);
    }

    def square(line: (String, Int)): (String, Long) = return ("1", line._2 * line._2)
    def total(line: (String, Int)): (Int, Long) = return (1, line._2)
    def populate(line: (String, Int)): (Int) = {
        var line1 = line._1.replace("(", "")
        line1 = line1.replace(")", "")
        var words = line1.split(",")
        var k = words(0).toInt
        var i = words(1).toInt - 4050
        var j = words(2).toInt - 7370
        var value = line._2.toInt
        data(i)(j)(k) = value
        return 1
    }
    def buildneighbors(date: Int, cellnox: Int, cellnoy: Int): Array[(String, Int)] = {
        var newDate = date - 1
        var newX = cellnox - 1
        var newY = cellnoy - 1
        var i = 0;
        var j = 0;
        var k = 0;
        var res: Array[(String, Int)] = Array()
        for (i <- 0 to 2) {
            for (j <- 0 to 2) {
                for (k <- 0 to 2) {
                    var date = newDate + i
                    var cellnox1 = newX + j
                    var cellnoy1 = newY + k
                    if ((cellnox1 >= 4050 && cellnox1 < 4090) && (cellnoy1 >= 7370 && cellnoy1 < 7425) && (date >= 0 && date < 31)) {

                        var temp = ((date, cellnox1, cellnoy1).toString, data(cellnox1 - 4050)(cellnoy1 - 7370)(date))
                        res = res :+ temp
                    }
                }
            }
        }
        return res
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Hotspot Analysis")

        val sc = new SparkContext(conf)

        val textfile = sc.textFile(args(0))

        val mainRdd = textfile.map(line => mapper(line)).groupBy(_._1).map(kv => (kv._1.toString, kv._2.map(_._2).sum)).filter(x => x._1 != "(-1,-1,-1)")

        var count = 68200

        val totalSum = mainRdd.map(line => total(line)).groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum)).filter(x => x._1 != "(-1,-1,-1)")
        val sum = totalSum.collect()(0)._2
        val squareSumRdd = mainRdd.map(line => square(line)).groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum)).filter(x => x._1 != "(-1,-1,-1)")
        val squareSum = squareSumRdd.collect()(0)._2
        var collectionRdd = mainRdd.collect()
        collectionRdd.foreach(populate)
        var z_scores = Array[(String, Double)]()

        for (i <- 4050 to 4090) {
            for (j <- 7370 to 7425) {
                for (k <- 0 until 31) {
                    var res = buildneighbors(k, i, j)
                    var res1 = new java.util.ArrayList[(String, Int)]
                    for (i <- 0 until res.length) {
                        res1.add(res.apply(i))
                    }
                    val score: Double = GetisCalculator.Hotspot(sc, res1, i, j, k, sum, squareSum, count)
                    var key = (i, j, k).toString
                    var z_score = (key, score)
                    z_scores = z_scores :+ z_score
                    val end = System.nanoTime

                }
            }
        }
        z_scores = z_scores.sortWith(_._2 > _._2).slice(0, 50)
        var z_scoresStr = Array[String]()
        for(i<- 0 until z_scores.length){
            var temp = z_scores(i).toString.replace("(","").replace(")","")
            var temp1 = temp.split(",")
            temp = temp1(0)+","+"-"+temp1(1)+","+temp1(2)+","+temp1(3)
            z_scoresStr = z_scoresStr :+ temp
        }
        val sortedRdd = sc.parallelize(z_scoresStr, 1)
        sortedRdd.saveAsTextFile(args(1))
        sc.stop()

    }
}




