import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.reflect.runtime.universe

case class Table(
  name: String,
  structure: org.apache.spark.sql.types.StructType)

object TPCHTables {
  val byName = Map(
    "customer" -> toStructType[Customer],
    "nation" -> toStructType[Nation],
    "lineitem" -> toStructType[Lineitem],
    "orders" -> toStructType[Order],
    "part" -> toStructType[Part],
    "partsupp" -> toStructType[Partsupp],
    "region" -> toStructType[Region],
    "supplier" -> toStructType[Supplier]
  ).map({case (name, struct) => (name, Table(name, struct))})

  def names = byName.keys
  def tables = byName.values

  def toStructType[caseClass: universe.TypeTag] = {
    catalyst.ScalaReflection
    .schemaFor[caseClass]
    .dataType
    .asInstanceOf[StructType]
  }

  // Courtesy of https://github.com/ssavvides/tpch-spark/
  // TPC-H table schemas
  case class Customer(
    c_custkey: Int,
    c_name: String,
    c_address: String,
    c_nationkey: Int,
    c_phone: String,
    c_acctbal: Double,
    c_mktsegment: String,
    c_comment: String)

  case class Lineitem(
    l_orderkey: Int,
    l_partkey: Int,
    l_suppkey: Int,
    l_linenumber: Int,
    l_quantity: Double,
    l_extendedprice: Double,
    l_discount: Double,
    l_tax: Double,
    l_returnflag: String,
    l_linestatus: String,
    l_shipdate: String,
    l_commitdate: String,
    l_receiptdate: String,
    l_shipinstruct: String,
    l_shipmode: String,
    l_comment: String)

  case class Nation(
    n_nationkey: Int,
    n_name: String,
    n_regionkey: Int,
    n_comment: String)

  case class Order(
    o_orderkey: Int,
    o_custkey: Int,
    o_orderstatus: String,
    o_totalprice: Double,
    o_orderdate: String,
    o_orderpriority: String,
    o_clerk: String,
    o_shippriority: Int,
    o_comment: String)

  case class Part(
    p_partkey: Int,
    p_name: String,
    p_mfgr: String,
    p_brand: String,
    p_type: String,
    p_size: Int,
    p_container: String,
    p_retailprice: Double,
    p_comment: String)

  case class Partsupp(
    ps_partkey: Int,
    ps_suppkey: Int,
    ps_availqty: Int,
    ps_supplycost: Double,
    ps_comment: String)

  case class Region(
    r_regionkey: Int,
    r_name: String,
    r_comment: String)

  case class Supplier(
    s_suppkey: Int,
    s_name: String,
    s_address: String,
    s_nationkey: Int,
    s_phone: String,
    s_acctbal: Double,
    s_comment: String)
}
