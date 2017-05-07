import org.apache.spark.sql._


object Main {
    def spark = SparkSession.builder()
                  .master("yarn")
                  .appName("JoinApp")
                  .getOrCreate()

    def main(args: Array[String]) {
        val folderNames =
            List(1, 10, 100).map(
                "file:///home/olojkine/master/dataset/" ++
                _.toString ++
                "/"
            )
        folderNames.foreach(new Converter(spark, _).convertAll())
    }
}
