import org.apache.spark.sql._

class Converter(spark: SparkSession, folderName: String) {

    def convert(table: Table) = {

        val baseName = folderName ++ table.name
        val inputFile = baseName ++ ".tbl"
        val outputFile = baseName ++ ".parquet"

        println("Converting " ++ inputFile ++ " to " ++ outputFile)

        try {
            spark.read
                .schema(table.structure)
                .option("delimiter", "|")
                .csv(inputFile)
                .write
                .mode("ignore")
                .option("compression", "none")
                .parquet(outputFile)
        } catch {
            case e: AnalysisException => {
                System.err.println(
                    "Unable to convert $inputFile : ${e.getSimpleMessage}"
                )
            }
        }
    }

    def convertAll() = {
        val tables = TPCHTables.all
        tables.foreach(convert)
    }
}
