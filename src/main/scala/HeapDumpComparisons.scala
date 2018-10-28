import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HeapComparisons {
  def filenameToPairRDD(session: SparkSession, name: String): RDD[(String, Long)] = {
    val file1RDD = session.sparkContext.textFile(name);
    file1RDD.map(l => {
      val items = l.split(" :INSTANCE-COUNT: ")
      (items(0), items(1).toLong)
    })
  }

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("HeapComparisons")
      .master("local")
      .getOrCreate()

    val rddOne = filenameToPairRDD(session, "/home/simon/JAMF/DataSets/MON-10-TABLE.TXT")
    val rddTwo = filenameToPairRDD(session, "/home/simon/JAMF/DataSets/WED-10-TABLE.TXT")
    val rddThree = filenameToPairRDD(session, "/home/simon/JAMF/DataSets/FRI-10-TABLE.TXT")

    //    val rddOne = filenameToPairRDD(session, "/home/simon/JAMF/DataSets/MON-04-TABLE.TXT")
    //    val rddTwo = filenameToPairRDD(session, "/home/simon/JAMF/DataSets/WED-04-TABLE.TXT")
    //    val rddThree = filenameToPairRDD(session, "/home/simon/JAMF/DataSets/FRI-04-TABLE.TXT")

    //        val rddOne = filenameToPairRDD(session, "/home/simon/JAMF/DataSets/MON-01-TABLE.TXT")
    //        val rddTwo = filenameToPairRDD(session, "/home/simon/JAMF/DataSets/WED-01-TABLE.TXT")
    //        val rddThree = filenameToPairRDD(session, "/home/simon/JAMF/DataSets/FRI-01-TABLE.TXT")

    val minInterestingCount = 5000
    val rejectSet = Set( // 50 classloaders???
      // most of these classes show 1, 1, 2800 instances. Is this all related, to what
      // activity, and would the count have dropped back down later if we had another sample?
      "Proxy",
      "DeployableObjectDataCacheManager",
      "PhantomReference",
      "ProxyLeakTask", // hikari connection pool
      "LoadQueryInfluencers" // hibernate engine spi
    );
    def cleaner(inRDD: RDD[(String, Long)]): RDD[(String, Long)] = {
      inRDD
        //          .filter(v => v._1.contains("Proxy"))
        .filter(v => rejectSet.forall(s => !v._1.contains(s)))
        .reduceByKey(_ + _)
    }

    val rddOneCleaned = cleaner(rddOne)
    val rddTwoCleaned = cleaner(rddTwo)
    val rddThreeCleaned = cleaner(rddThree)

    val rddI1 = rddOneCleaned.fullOuterJoin(rddTwoCleaned)
      .mapValues(ov => (ov._1.getOrElse(0L), ov._2.getOrElse(0L)))
      .fullOuterJoin(rddThreeCleaned)
      .mapValues(ov => (ov._1.getOrElse((0L, 0L)), ov._2.getOrElse(0L)))
      .mapValues(ov => (ov._1._1, ov._1._2, ov._2))
    // should now be fully joined, with zeroes in place of missing

    val rddI2 = rddI1
      .filter(r => r._2._1 > minInterestingCount
        || r._2._2 > minInterestingCount
        || r._2._3 > minInterestingCount) // only interesting with some occurrences
      .mapValues(v => (v._1, v._2, v._3, v._3.toDouble / v._2, v._2.toDouble / v._1, v._3.toDouble / v._1))
      .filter(v => v._2._4 > 0 && v._2._5 > 0) // continuously increasing
      .filter(v => v._2._6 > 1.2)
      .sortBy(v => v._2._6, false)
      .take(250)
      .foreach(p => println(
        f"${p._2._1}%7d : ${p._2._2}%7d : ${p._2._3}%7d " +
          f"growth ${p._2._4*100}%6.1f%%, ${p._2._5*100}%6.1f%%, ${p._2._6*100}%6.1f%%" +
          f" : ${p._1}"))

    session.close()
  }
}
