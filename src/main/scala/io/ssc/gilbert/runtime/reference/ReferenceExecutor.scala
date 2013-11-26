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

package io.ssc.gilbert.runtime.reference

import io.ssc.gilbert._
import org.apache.mahout.math.{DenseMatrix, SparseRowMatrix}
import io.ssc.gilbert.runtime.VectorFunctions
import io.ssc.gilbert.AggregateMatrixTransformation
import io.ssc.gilbert.MatrixMult
import io.ssc.gilbert.CellwiseMatrixTransformation
import io.ssc.gilbert.WriteMatrix
import io.ssc.gilbert.Transpose
import io.ssc.gilbert.LoadMatrix
import io.ssc.gilbert.ScalarMatrixTransformation
import org.apache.mahout.math.random.Normal
import io.ssc.gilbert.optimization.CommonSubexpressionDetector
import io.ssc.gilbert.shell.{local, printPlan}

import scala.io.Source

object ReferenceExecutorRunner {

  def main(args: Array[String]): Unit = {

    val A = load("/home/ssc/Desktop/gilbert/test/matrix.tsv", 3, 3)

    val b = ones(3, 1) / math.sqrt(3)

    local(norm(A * b, 2))
  }
}

class ReferenceExecutor extends Executor {

  type MahoutMatrix = org.apache.mahout.math.Matrix

  def run(executable: Executable) = {

    setRedirects(new CommonSubexpressionDetector().find(executable))

    printPlan(executable)

    execute(executable)
  }

  //TODO fix this
  var iterationState: MahoutMatrix = null

  protected def execute(executable: Executable): Any = {

    executable match {
      case (transformation: LoadMatrix) => {

        handle[LoadMatrix, Unit](transformation,
            { _ => },
            { (transformation, _) => {

              val matrix = new SparseRowMatrix(transformation.numRows, transformation.numColumns)

              for (line <- Source.fromFile(transformation.path).getLines()) {
                val fields = line.split(" ")
                matrix.setQuick(fields(0).toInt - 1, fields(1).toInt - 1, fields(2).toDouble)
              }

              matrix
            }})
      }

      case (transformation: FixpointIteration) => {

        iterationState = handle[FixpointIteration, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.initialState) },
            { (_, initialVector) => initialVector }).asInstanceOf[MahoutMatrix]

        for (_ <- 1 to 10) {
          iterationState = handle[FixpointIteration, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.updatePlan) },
            { (_, vector) => vector }).asInstanceOf[MahoutMatrix]
        }

        iterationState
      }

      case (transformation: IterationStatePlaceholder) => { iterationState }

      case (transformation: CellwiseMatrixTransformation) => {

        handle[CellwiseMatrixTransformation, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.matrix) },
            { (transformation, matrix) => {
              transformation.operation match {
                case ScalarOperation.Binarize => { matrix.assign(VectorFunctions.binarize) }
              }
            }})
      }

      case (transformation: Transpose) => {

        handle[Transpose, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.matrix) },
            { (transformation, matrix) => matrix.transpose() })
      }

      case (transformation: MatrixMult) => {

        handle[MatrixMult, (MahoutMatrix, MahoutMatrix)](transformation,
            { transformation => {
              (evaluate[MahoutMatrix](transformation.left), evaluate[MahoutMatrix](transformation.right))
            }},
            { case (_, (leftMatrix, rightMatrix)) => leftMatrix.times(rightMatrix) })
      }

      case (transformation: AggregateMatrixTransformation) => {

        handle[AggregateMatrixTransformation, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.matrix) },
            { (transformation, matrix) => {
              transformation.operation match {
                case ScalarsOperation.Maximum => { matrix.aggregate(VectorFunctions.max, VectorFunctions.identity) }
                case ScalarsOperation.Norm2 => {
                  val sumOfSquaredEntries = matrix.aggregateRows(VectorFunctions.lengthSquared).zSum()
                  math.sqrt(sumOfSquaredEntries)
                }
              }
            }})
      }

      case (transformation: ScalarMatrixTransformation) => {

        handle[ScalarMatrixTransformation, (MahoutMatrix, Double)](transformation,
            { transformation => {
              (evaluate[MahoutMatrix](transformation.matrix), (evaluate[Double](transformation.scalar)))
            }},
            { case (transformation, (matrix, scalar)) => {
              transformation.operation match {
                case (ScalarsOperation.Division) => { matrix.divide(scalar) }
                case (ScalarsOperation.Multiplication) => { matrix.times(scalar) }
              }
            }})
      }

      case (transformation: VectorwiseMatrixTransformation) => {

        handle[VectorwiseMatrixTransformation, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.matrix) },
            { (transformation, matrix) => {
              transformation.operation match {
                case (VectorwiseOperation.NormalizeL1) => {
                  for (index <- 0 until matrix.numRows()) {
                    matrix.viewRow(index).normalize(1)
                  }
                }
                matrix
              }
            }})
      }

      case (transformation:  CellwiseMatrixMatrixTransformation) => {

        handle[CellwiseMatrixMatrixTransformation, (MahoutMatrix, MahoutMatrix)](transformation,
            { transformation => {
              (evaluate[MahoutMatrix](transformation.left), evaluate[MahoutMatrix](transformation.right))
            }},
            { case (transformation, (leftMatrix, rightMatrix)) => {
              transformation.operation match {
                case CellwiseOperation.Addition => leftMatrix.plus(rightMatrix)
              }
            }})
      }

      case (transformation: ones) => {

        handle[ones, Unit](transformation,
            { _ => },
            { (transformation, _) => { new DenseMatrix(transformation.rows, transformation.columns).assign(1) }})
      }

      case (transformation: rand) => {

        handle[rand, Unit](transformation,
            { _ => },
            { (transformation, _) => {
              new DenseMatrix(transformation.rows, transformation.columns)
                 .assign(new Normal(transformation.mean, transformation.std))
            }})
      }

      case (transformation: WriteMatrix) => {

        handle[WriteMatrix, MahoutMatrix](transformation,
            { transformation => evaluate[MahoutMatrix](transformation.matrix) },
            { (_, matrix) => println(matrix) })
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

