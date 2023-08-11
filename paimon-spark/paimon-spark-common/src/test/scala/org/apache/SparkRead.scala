package org.apache

import org.apache.paimon.spark.{PaimonSparkSessionExtension, SparkCatalog}
import org.apache.spark.sql.SparkSession

object SparkRead {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sql("show tables").show()
    sparkSession.sql("desc tb_all_type_without_zone").show()
    sparkSession.sql("show create table tb_all_type_without_zone").show()
    sparkSession.sql("show paritions from tb_all_type_without_zone").show()
    sparkSession.sql("show paritions from tb_all_type").show()

    //    sparkSession.sql("select * from tb_all_type_without_zone").show()
//    sparkSession.sql("delete from tb_all_type_without_zone where dt='20230810' and hr='1550' ").show()

  }
}
