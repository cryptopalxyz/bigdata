package sparksqlcourse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Repartition, RepartitionByExpression}
import org.apache.spark.sql.catalyst.rules.Rule

/*
injectOptimizerRule – 添加optimizer自定义规则，optimizer负责逻辑执行计划的优化,我们例子中就是扩展了逻辑优化规则。
injectParser – 添加parser自定义规则，parser负责SQL解析。
injectPlannerStrategy – 添加planner strategy自定义规则，planner负责物理执行计划的生成。
injectResolutionRule – 添加Analyzer自定义规则到Resolution阶段，analyzer负责逻辑执行计划生成。
injectPostHocResolutionRule – 添加Analyzer自定义规则到Post Resolution阶段。
injectCheckRule – 添加Analyzer自定义Check规则。
*/

case class MyRule(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transform  {
    case _ : LogicalPlan =>  {
      logError("geek time1!")
      logWarning("geek time2!")
      print("geek time3!")
      System.err.println("geek time4")
      plan }

  }


}