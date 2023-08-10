package org.apache

import org.apache.paimon.spark.{PaimonSparkSessionExtension, SparkCatalog}
import org.apache.spark.sql.SparkSession

object SparkSelfTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.catalog.paimon", classOf[SparkCatalog].getName)
      .config("spark.sql.catalog.paimon.warehouse", "/tmp/paimon")
      .config("spark.sql.extensions", classOf[PaimonSparkSessionExtension].getName)
      .getOrCreate()
    sparkSession.catalog.setCurrentCatalog("paimon")
    sparkSession.catalog.setCurrentDatabase("default")
    sparkSession.sql("show tables").show()
    sparkSession.sql("desc tb_all_type").show()
    sparkSession.sql("alter table `tb_all_type` drop partition (dt='1111', hr='222')").show()
    sparkSession.sql("show partitions tb_all_type").show()

    //    sparkSession.sql("desc")
  }
}
