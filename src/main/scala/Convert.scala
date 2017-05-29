import org.apache.spark.sql._
import Main.{spark, logger}

object Converter {
  def tblFileName(tableName:String) = "file:///tmp/dataset/" ++ tableName ++ ".tbl"
  def parquetFileName(tableName:String) = tableName ++ ".parquet"

  def readFile(table: Table): DataFrame = {
    spark.read
    .schema(table.structure)
    .option("delimiter", "|")
    .csv(tblFileName(table.name))
  }

  def read(tableName: String) : DataFrame = {
    TPCHTables.byName.get(tableName) match {
      case None => throw new Exception("No such table: " ++ tableName)
      case Some(table) => readFile(table)
    }
  }

  def write(df: DataFrame, tableName: String) = {
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(parquetFileName(tableName))
  }

  def convert(tableName: String) = {
    logger.info(s"Converting ${tableName}...")
    try {
      write(read(tableName), tableName)
    } catch {
      case e: AnalysisException => {
        System.err.println(
          s"Unable to convert ${tableName}: ${e.getSimpleMessage}"
        )
      }
    }
  }
}
