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

package io.ssc.gilbert2.runtime.spark

import io.ssc.gilbert2._
import org.apache.mahout.math._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import io.ssc.gilbert2.runtime.VectorFunctions
import scala.collection.JavaConversions._
import org.apache.mahout.math.Vector
import io.ssc.gilbert2.CellwiseMatrixTransformation
import io.ssc.gilbert2.WriteMatrix
import io.ssc.gilbert2.Transpose
import io.ssc.gilbert2.LoadMatrix

import org.apache.mahout.math.function.Functions
import org.apache.mahout.math.random.Normal
import io.ssc.gilbert2.optimization.CommonSubexpressionDetector
import io.ssc.gilbert2.shell.{printPlan, withSpark, local}

object SparkExecutorRunner {

  def main(args: Array[String]): Unit = {
    val A = load("/home/ssc/Desktop/gilbert/test/matrix.tsv")

    val B = A.binarize()

    val C = B.t * B

    val D = C / C.max()

    withSpark(D)
  }
}

class SparkExecutor extends Executor {
  
  type RowPartitionedMatrix = RDD[(Int, Vector)]

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //System.setProperty("spark.kryo.registrator", classOf[DenseVector].getName)
  System.setProperty("spark.kryo.referenceTracking", "false")
  System.setProperty("spark.kryoserializer.buffer.mb", "8")
  System.setProperty("spark.locality.wait", "10000")

  val sc = new SparkContext("local", "Gilbert")
  val degreeOfParallelism = 2

  def run(executable: Executable) = {

    setRedirects(new CommonSubexpressionDetector().find(executable))

    printPlan(executable)

    execute(executable)
  }

  protected def execute(executable: Executable): Any = {

    executable match {

      case (transformation: LoadMatrix) => {

        handle[LoadMatrix, Unit](transformation,
            { _ => },
            { (transformation, _) => {
              sc.textFile(transformation.path, degreeOfParallelism).map({ line => {
                val fields = line.split(" ")
                (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
              }}).groupBy(_._1).flatMap({ case (index, elements) => {
                val vector = new RandomAccessSparseVector(Integer.MAX_VALUE)
                for ((_, column, value) <- elements) {
                  vector.setQuick(column, value)
                }
                Seq((index, new SequentialAccessSparseVector(vector)))
              }})
            }})
      }

      case (transformation: CellwiseMatrixTransformation) => {

        handle[CellwiseMatrixTransformation, RowPartitionedMatrix](transformation,
            { transformation => evaluate[RowPartitionedMatrix](transformation.matrix) },
            { (transformation, matrix) => {
              transformation.operation match {
                case ScalarOperation.Binarize => {
                  //TODO add binarize to mahout to only apply it to non zeros!
                  matrix.map({ case (index, row) => (index, row.assign(VectorFunctions.binarize)) })
                }
              }
            }})
      }

      case (transformation: AggregateMatrixTransformation) => {

        handle[AggregateMatrixTransformation, RowPartitionedMatrix](transformation,
            { transformation => evaluate[RowPartitionedMatrix](transformation.matrix) },
            { (transformation, matrix) => {
              transformation.operation match {
                case ScalarsOperation.Maximum => {
                  matrix.map({ case (index, row) => row.maxValue() }).aggregate(Double.MinValue)(math.max, math.max)
                }
              }
            }})
      }

      case (transformation: Transpose) => {

        handle[Transpose, RowPartitionedMatrix](transformation,
            { transformation => evaluate[RowPartitionedMatrix](transformation.matrix)},
            { (transformation, matrix) => {
              //TODO make reduce combinable
              matrix.flatMap({ case (index, row) => {
                for (elem <- row.nonZeroes())
                yield { (elem.index(), index, elem.get()) }
              }})
                .groupBy(_._1).map({ case (index, entries) => {
                val row = new RandomAccessSparseVector(Integer.MAX_VALUE)
                entries.foreach({ case (_, columnIndex, value) => row.setQuick(columnIndex, value) })
                (index, new SequentialAccessSparseVector(row))
              }})
            }})
      }

       //TODO we should eliminate transpose before
      case (transformation: MatrixMult) => {

        handle[MatrixMult, (RowPartitionedMatrix, RowPartitionedMatrix)](transformation,
            { transformation => {
              val leftMatrix = evaluate[RowPartitionedMatrix](transformation.left)
              val rightMatrix = evaluate[RowPartitionedMatrix](transformation.right)
              (leftMatrix, rightMatrix)
            }},
            { case (_, (leftMatrix, rightMatrix)) => {

              //TODO make reduce combinable
              /* row outer product formulation of matrix multiplication */
              val transposedLeftMatrix = leftMatrix.flatMap({ case (index, row) => {
                  for (elem <- row.nonZeroes())
                    yield { (elem.index(), index, elem.get()) }
                }})
                .groupBy(_._1).map({ case (index, entries) => {
                  val row = new RandomAccessSparseVector(Integer.MAX_VALUE)
                  entries.foreach({ case (_, columnIndex, value) => row.setQuick(columnIndex, value) })
                  (index, new SequentialAccessSparseVector(row))
                }})

              transposedLeftMatrix.join(rightMatrix).flatMap({ case (_, (column, row)) => {
                for (elem <- column.nonZeroes())
                  yield { (elem.index(), row.times(elem.get())) }
              }})
              .reduceByKey(_.assign(_, Functions.PLUS))

            }})
      }

      case (transformation: ScalarMatrixTransformation) => {

        handle[ScalarMatrixTransformation, (RowPartitionedMatrix, Double)](transformation,
          { transformation => {
            (evaluate[RowPartitionedMatrix](transformation.matrix), evaluate[Double](transformation.scalar))
          }},
          { case (transformation, (matrix, value)) => {
            transformation.operation match {
              case (ScalarsOperation.Division) => {
                matrix.map({ case (index, row) => (index, row.assign(Functions.DIV, value)) })
              }
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


      case (transformation: ScalarVectorTransformation) => {

        handle[ScalarVectorTransformation, (Vector, Double)](transformation,
            { transformation => (evaluate[Vector](transformation.vector), evaluate[Double](transformation.scalar)) },
            { case (transformation, (vector, scalar)) => {
              transformation.operation match {
                case (ScalarsOperation.Division) => { vector.assign(Functions.DIV, scalar) }
              }
            }})
      }

      case (transformation: VectorAggregationTransformation) => {

        handle[VectorAggregationTransformation, Vector](transformation,
        { transformation => evaluate[Vector](transformation.vector) },
        { (transformation, vector) => {
          transformation.operation match {
            case (VectorwiseOperation.Norm2Squared) => vector.norm(2)
            case (VectorwiseOperation.Max) => vector.maxValue()
            case (VectorwiseOperation.Min) => vector.minValue()
            case (VectorwiseOperation.Average) => vector.zSum() / vector.size()
          }
        }})
      }

      //TODO we need a better way to do this, then copying to an array on the driver
      case (transformation: MatrixToVectorTransformation) => {

        handle[MatrixToVectorTransformation, RowPartitionedMatrix](transformation,
            { transformation => evaluate[RowPartitionedMatrix](transformation.matrix) },
            { (transformation, matrix) => {
              transformation.operation match {
                case (MatrixwiseOperation.RowSums) => {
                  val sums = matrix.map({ case (index, row) => (index, row.zSum()) }).toArray()
                  val vector = new RandomAccessSparseVector(Integer.MAX_VALUE)
                  for ((row, sum) <- sums) {
                    vector.setQuick(row, sum)
                  }
                  new SequentialAccessSparseVector(vector)
                }
              }
            }})
      }

      case (transformation: WriteMatrix) => {

        handle[WriteMatrix, RowPartitionedMatrix](transformation,
            { transformation => evaluate[RowPartitionedMatrix](transformation.matrix) },
            { (_, matrix) => {
              matrix.foreach({ case (index, row) =>
                println(index + " " + row)
              })
            }})
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
            { (transformation, value) => println(value) })
      }
    }

  }

}
