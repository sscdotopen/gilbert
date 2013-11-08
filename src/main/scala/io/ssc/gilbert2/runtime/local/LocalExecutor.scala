/**
 * gilbert - Distributed Linear Algebra on Sparse Matrices
 * Copyright (C) 2013  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.gilbert2.runtime.local

import io.ssc.gilbert2._
import org.apache.mahout.math.{DenseVector, Vector, SparseRowMatrix}
import org.apache.mahout.math.function.Functions
import io.ssc.gilbert2.runtime.VectorFunctions
import io.ssc.gilbert2.AggregateMatrixTransformation
import io.ssc.gilbert2.MatrixMult
import io.ssc.gilbert2.CellwiseMatrixTransformation
import io.ssc.gilbert2.WriteMatrix
import io.ssc.gilbert2.Transpose
import io.ssc.gilbert2.LoadMatrix
import io.ssc.gilbert2.ScalarMatrixTransformation
import org.apache.mahout.math.random.Normal
import io.ssc.gilbert2.optimization.CommonSubexpressionDetector
import io.ssc.gilbert2.shell.{local, printPlan}

import scala.io.Source

object LocalExecutorRunner {

  def main(args: Array[String]): Unit = {
    val A = load("/home/ssc/Desktop/gilbert/test/matrix.tsv")

    val B = A.binarize()

    val C = B.t * B

    val D = C / C.max()

    local(D)
  }
}

class LocalExecutor extends Executor {


  def run(executable: Executable) = {

    setRedirects(new CommonSubexpressionDetector().find(executable))

    printPlan(executable)

    execute(executable)
  }

  //TODO refactor
  protected def execute(executable: Executable): Any = {

    executable match {
      case (transformation: LoadMatrix) => {

        handle[LoadMatrix, Unit](transformation,
            { _ => },
            { (transformation, _) => {

              //TODO nicer
              var entries = Seq[(Int, Int, Double)]()
              for (line <- Source.fromFile(transformation.path).getLines()) {
                val fields = line.split(" ")
                entries = entries ++ Seq((fields(0).toInt, fields(1).toInt, fields(2).toDouble))
              }

              val maxColumn = entries.map({ case (_, column, _) => column }).reduce(math.max)
              val maxRow = entries.map({ case (row, _, _) => row }).reduce(math.max) 

              val matrix = new SparseRowMatrix(maxRow + 1, maxColumn + 1)

              entries.foreach({ case (row, column, value) => matrix.setQuick(row, column, value) })

              matrix
            }})
      }

      case (transformation: CellwiseMatrixTransformation) => {

        handle[CellwiseMatrixTransformation, SparseRowMatrix](transformation,
            { transformation => evaluate[SparseRowMatrix](transformation.matrix) },
            { (transformation, matrix) => {
              transformation.operation match {
                case ScalarOperation.Binarize => { matrix.assign(VectorFunctions.binarize) }
              }
            }})
      }

      case (transformation: Transpose) => {

        handle[Transpose, SparseRowMatrix](transformation,
            { transformation => evaluate[SparseRowMatrix](transformation.matrix) },
            { (transformation, matrix) => matrix.transpose() })
      }

      case (transformation: MatrixMult) => {

        handle[MatrixMult, (SparseRowMatrix, SparseRowMatrix)](transformation,
            { transformation => {
              (evaluate[SparseRowMatrix](transformation.left), evaluate[SparseRowMatrix](transformation.right))
            }},
            { case (_, (leftMatrix, rightMatrix)) => leftMatrix.times(rightMatrix) })
      }

      case (transformation: AggregateMatrixTransformation) => {

        handle[AggregateMatrixTransformation, SparseRowMatrix](transformation,
            { transformation => evaluate[SparseRowMatrix](transformation.matrix) },
            { (transformation, matrix) => {
              transformation.operation match {
                case (ScalarsOperation.Maximum) => { matrix.aggregate(VectorFunctions.max, VectorFunctions.identity) }
              }
            }})
      }

      case (transformation: ScalarMatrixTransformation) => {

        handle[ScalarMatrixTransformation, (SparseRowMatrix, Double)](transformation,
            { transformation => {
              (evaluate[SparseRowMatrix](transformation.matrix), (evaluate[Double](transformation.scalar)))
            }},
            { case (transformation, (matrix, scalar)) => {
              transformation.operation match {
                case (ScalarsOperation.Division) => { matrix.divide(scalar) }
              }
            }})
      }

      case (transformation: MatrixToVectorTransformation) => {

        handle[MatrixToVectorTransformation, SparseRowMatrix](transformation,
            { transformation => evaluate[SparseRowMatrix](transformation.matrix) },
            { (transformation, matrix) => {
              transformation.operation match {
                case (MatrixwiseOperation.RowSums) => { matrix.aggregateRows(VectorFunctions.sum) }
              }
            }})
      }

      case (transformation: ScalarVectorTransformation) => {

        handle[ScalarVectorTransformation, (Vector, Double)](transformation,
            { transformation => (evaluate[Vector](transformation.vector), evaluate[Double](transformation.scalar)) },
            { case (transformation, (vector, scalar)) => {
              transformation.operation match {
                case (ScalarsOperation.Division) => { vector.assign(Functions.DIV, scalar) }
              }
            }})
      }

      case (transformation: ones) => {

        handle[ones, Unit](transformation,
            { _ => },
            { (transformation, _) => new DenseVector(transformation.size).assign(1) })
      }

      case (transformation: rand) => {

        handle[rand, Unit](transformation,
            { _ => },
            { (transformation, _) => {
              new DenseVector(transformation.size).assign(new Normal(transformation.mean, transformation.std))
            }})
      }

      case (transformation: WriteMatrix) => {

        handle[WriteMatrix, SparseRowMatrix](transformation,
            { transformation => evaluate[SparseRowMatrix](transformation.matrix) },
            { (_, matrix) => println(matrix) })
      }

      case (transformation: WriteVector) => {

        handle[WriteVector, Vector](transformation,
            { transformation => evaluate[Vector](transformation.vector) },
            { (_, vector) => println(vector) })
      }

      case (transformation: scalar) => {

        handle[scalar, Unit](transformation,
            { _ => },
            { (transformation, _) => transformation.value })
      }

      case (transformation: WriteScalarRef) => {

        handle[WriteScalarRef, Double](transformation,
            { transformation => evaluate[Double](transformation.scalar) },
            { (_, scalar) => println(scalar) })
      }        
    }

  }
}

