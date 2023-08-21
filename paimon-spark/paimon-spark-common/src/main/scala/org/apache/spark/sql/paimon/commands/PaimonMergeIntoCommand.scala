package org.apache.spark.sql.paimon.commands

import org.apache.paimon.spark.SparkTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeIntoTable, SubqueryAlias}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

case class PaimonMergeIntoCommand(mergeIntoTable: MergeIntoTable) extends RunnableCommand {
  private var sparkSession: SparkSession = _

  override def run(sparkSession: SparkSession): Seq[Row] = {
    this.sparkSession = sparkSession
    val table = mergeIntoTable.targetTable
    Seq.empty[Row]
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    this
  }
}

object PaimonMergeIntoCommand {
  def isPaimonTable(loginPlan: LogicalPlan): Boolean = {
    loginPlan match {
      case SubqueryAlias(_, relation: LogicalPlan) =>
        isPaimonTable(relation)
      case DataSourceV2Relation(table, _, _, _, _) => table.isInstanceOf[SparkTable]
      case _ => false
    }
  }
}