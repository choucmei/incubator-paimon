package org.apache

import org.apache.paimon.spark.{PaimonSparkSessionExtension, SparkCatalog}
import org.apache.spark.sql.SparkSession

object SparkReadFile {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.catalog.paimon", classOf[SparkCatalog].getName)
      .config("spark.sql.catalog.paimon.warehouse", "hdfs://slave1:8020/user/hive/paimon/")
      .config("spark.sql.extensions", classOf[PaimonSparkSessionExtension].getName)
      .getOrCreate()

    sparkSession.sql("show tables").show()
    sparkSession.sql("show catalogs").show()
    sparkSession.sql("USE paimon").show()
    sparkSession.sql("USE default").show()
    sparkSession.sql("show tables").show()
//    sparkSession.sql("select * from tb_all_type_without_zone").show()
    sparkSession.sql("desc tb_all_type_without_zone").show()
    sparkSession.sql("show create table tb_all_type_without_zone").show()
    sparkSession.sql("SHOW PARTITIONS tb_all_type_without_zone").show()
    sparkSession.sql("SHOW PARTITIONS tb_all_type").show()


    //    sparkSession.sql("delete from tb_all_type_without_zone where dt='20230810' and hr='1550' ").show()
//    sparkSession.sql("delete from tb_all_type_without_zone where id=-9209352040626360320 ").show()
  }
}
