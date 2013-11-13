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
import org.apache.mahout.math.{DenseVector2, Vector, SparseRowMatrix}
import org.apache.mahout.math.function.Functions
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

    val x_0 = ones(3) / scalar(1 / math.sqrt(3))

    val eigenvector = fixpoint(x_0, { x => (A * x) / norm2(A * x) })

    local(eigenvector)
  }
}

class ReferenceExecutor extends Executor {


  def run(executable: Executable) = {

    setRedirects(new CommonSubexpressionDetector().find(executable))

    //print(new CommonSubexpressionDetector().find(executable))

    printPlan(executable)

    execute(executable)
  }

  //TODO fix this
  var iterationState: Vector = null

  //TODO refactor
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

        iterationState = handle[FixpointIteration, Vector](transformation,
            { transformation => evaluate[Vector](transformation.initialState) },
            { (_, initialVector) => initialVector }).asInstanceOf[Vector]

        for (_ <- 1 to 10) {
          iterationState = handle[FixpointIteration, Vector](transformation,
            { transformation => evaluate[Vector](transformation.updatePlan) },
            { (_, vector) => vector }).asInstanceOf[Vector]
        }

        iterationState
      }

      case (transformation: IterationStatePlaceholder) => { iterationState }

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
                case (ScalarsOperation.Multiplication) => { matrix.times(scalar) }
              }
            }})
      }

      case (transformation: MatrixVectorMult) => {

        handle[MatrixVectorMult, (SparseRowMatrix, Vector)](transformation,
            { transformation => {
              (evaluate[SparseRowMatrix](transformation.matrix), evaluate[Vector](transformation.vector))
            }},
            { case (_, (matrix, vector)) => matrix.times(vector) })
      }

      case (transformation: VectorwiseMatrixTransformation) => {

        handle[VectorwiseMatrixTransformation, SparseRowMatrix](transformation,
            { transformation => evaluate[SparseRowMatrix](transformation.matrix) },
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
                case (ScalarsOperation.Multiplication) => { vector.assign(Functions.MULT, scalar) }
              }
            }})
      }

      case (transformation: VectorAggregationTransformation) => {

        handle[VectorAggregationTransformation, Vector](transformation,
            { transformation => evaluate[Vector](transformation.vector) },
            { (transformation, vector) => {
              transformation.operation match {
                case (VectorwiseOperation.Norm2Squared) => {
                  val norm = vector.norm(2)
                  norm * norm
                }
                case (VectorwiseOperation.Norm2) => vector.norm(2)
                case (VectorwiseOperation.Max) => vector.maxValue()
                case (VectorwiseOperation.Min) => vector.minValue()
                case (VectorwiseOperation.Average) => vector.zSum() / vector.size()
              }
            }})
      }

      case (transformation: CellwiseVectorTransformation) => {
        handle[CellwiseVectorTransformation, (Vector, Vector)](transformation,
            { transformation => (evaluate[Vector](transformation.left), evaluate[Vector](transformation.right)) },
            { case (transformation, (left, right)) => {
              transformation.operation match {
                case (CellwiseOperation.Addition) => { left.assign(right, Functions.PLUS) }
              }
            }})
      }

      case (transformation: ones) => {

        handle[ones, Unit](transformation,
            { _ => },
            { (transformation, _) => new DenseVector2(transformation.size).assign(1) })
      }

      case (transformation: rand) => {

        handle[rand, Unit](transformation,
            { _ => },
            { (transformation, _) => {
              new DenseVector2(transformation.size).assign(new Normal(transformation.mean, transformation.std))
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

