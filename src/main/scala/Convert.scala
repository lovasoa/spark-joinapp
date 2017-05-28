import org.apache.spark.sql._

class Converter(spark: SparkSession, folderName: String) {

  def baseName(table: Table) = folderName ++ table.name

  def read(table: Table) = {
    val inputFile = baseName(table) ++ ".tbl"
    spark.read
    .schema(table.structure)
    .option("delimiter", "|")
    .csv(inputFile)
  }

  def convert(table: Table) = {
    val outputFile = baseName(table) ++ ".parquet"

    println("Converting " ++ table.name ++ " to " ++ outputFile)

    try {
      read(table)
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
    val tables = TPCHTables.tables
    tables.foreach(convert)
  }
}
