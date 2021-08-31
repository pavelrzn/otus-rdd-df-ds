package homework2

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

object UDFLib {


  def processRmsd: UserDefinedFunction = udf((distancesWrArr: mutable.WrappedArray[Double], avgDist: Double) => {
    val distances = distancesWrArr
    def rmsd: Double = Math.sqrt(distances
      .map(dist => Math.pow(dist - avgDist, 2))
      .sum / distances.length)

    distances.length match {
      case 0 => 0
      case _ => rmsd
    }

  })
}
