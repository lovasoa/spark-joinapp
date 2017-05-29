import org.apache.spark.sql._
import Main.{spark, folderName}

object Converter {
  def baseName(table: Table) = folderName ++ table.name

  def read(table: Table) : DataFrame = {
    val inputFile = baseName(table) ++ ".tbl"
    spark.read
    .schema(table.structure)
    .option("delimiter", "|")
    .csv(inputFile)
  }

  def read(tableName: String) : DataFrame = {
    TPCHTables.byName.get(tableName) match {
      case None => throw new Exception("No such table: " ++ tableName)
      case Some(table) => read(table)
    }
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
          s"Unable to convert $inputFile : ${e.getSimpleMessage}"
        )
      }
    }
  }

  def convertAll() = {
    val tables = TPCHTables.tables
    tables.foreach(convert)
  }
}
