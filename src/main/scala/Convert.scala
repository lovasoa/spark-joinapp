import org.apache.spark.sql._
import Main.{spark, logger}

object Converter {
  def parquetFileName(tableName:String) = tableName ++ ".parquet"

  def write(df: DataFrame, tableName: String) = {
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(parquetFileName(tableName))
  }
}

class Converter(folder: String) {
  def tblFileName(tableName:String) = s"$folder/$tableName.tbl"

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

  def convert(tableName: String) = {
    logger.info(s"Converting $tableName in $folder...")
    Converter.write(read(tableName), tableName)
  }
}
