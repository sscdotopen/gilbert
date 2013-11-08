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

object SparkExecutor extends Executor {

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", classOf[DenseVector].getName)
  System.setProperty("spark.kryo.referenceTracking", "false")
  System.setProperty("spark.kryoserializer.buffer.mb", "8")
  System.setProperty("spark.locality.wait", "10000")

  val sc = new SparkContext("local", "Gilbert")
  val degreeOfParallelism = 2


  def main(args: Array[String]): Unit = {
    run(Examples.cooccurrences)
  }

  def run(executable: Executable) = {
    execute(executable)
  }

  protected def execute(executable: Executable): Any = {

    executable match {
      case (op: LoadMatrix) => {

        sc.textFile(op.path, degreeOfParallelism).map({ line => {
          val fields = line.split(" ")
          (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
        }}).groupBy(_._1).flatMap({ case (index, elements) => {
          val vector = new RandomAccessSparseVector(Integer.MAX_VALUE)
          for ((_, column, value) <- elements) {
            vector.setQuick(column, value)
          }
          Seq((index, new SequentialAccessSparseVector(vector)))
        }})
      }

      case (op: CellwiseMatrixTransformation) => {
        val matrix = evaluate(op.matrix)

        op.operation match {
          case ScalarOperation.Binarize => {

            matrix.map({ case (index, row) => {
              //TODO add binarize to mahout to only apply it to non zeros!
              (index, row.assign(VectorFunctions.binarize))
            }})
          }
        }
      }

      case (op: AggregateMatrixTransformation) => {
        val matrix = evaluate(op.matrix)

        op.operation match {
          case ScalarsOperation.Maximum => {
            matrix.map({ case (index, row) => row.maxValue() }).aggregate(Double.MinValue)(math.max, math.max)
          }
        }
      }

      case (op: Transpose) => {
        val matrix = evaluate(op.matrix)

        matrix.flatMap({ case (index, row) => {
            for (elem <- row.nonZeroes())
              yield { (elem.index(), index, elem.get()) }
          }})
          .groupBy(_._1).map({ case (index, entries) => {
            val row = new RandomAccessSparseVector(Integer.MAX_VALUE)
            entries.foreach({ case (_, columnIndex, value) => row.setQuick(columnIndex, value) })
            (index, new SequentialAccessSparseVector(row))
          }})
      }

      case (op: MatrixMult) => {

        /* row outer product formulation of matrix multiplication */
        val leftMatrix = evaluate(Transpose(op.left))
        val rightMatrix = evaluate(op.right)

        leftMatrix.join(rightMatrix).flatMap({ case (_, (column, row)) => {
            for (elem <- column.nonZeroes())
              yield { (elem.index(), row.times(elem.get())) }
          }})
          .reduceByKey(_.assign(_, Functions.PLUS))
      }

      case (op: ScalarMatrixTransformation) => {
        val matrix = evaluate(op.matrix)
        val scalar = evaluateAsScalar(op.scalar)

        op.operation match {
          case (ScalarsOperation.Division) => {
            matrix.map({ case (index, row) => (index, row.assign(Functions.DIV, scalar)) })
          }
        }
      }

      case (op: ones) => {
        new DenseVector(op.size).assign(1)
      }

      case (op: rand) => {
        new DenseVector(op.size).assign(new Normal(op.mean, op.std))
      }

      case (op: ScalarVectorTransformation) => {
        val vector = evaluateAsVector(op.vector)
        val scalar = evaluateAsScalar(op.scalar)

        op.operation match {
          case (ScalarsOperation.Division) => {
            vector.assign(Functions.DIV, scalar)
          }
        }
      }

      //TODO we need a better way to do this, then copying to an array on the driver
      case (op: MatrixToVectorTransformation) => {

        val matrix = evaluate(op.matrix)

        op.operation match {
          case (MatrixwiseOperation.RowSums) => {
            val sums = matrix.map({ case (index, row) => (index, row.zSum()) }).toArray()
            val vector = new RandomAccessSparseVector(Integer.MAX_VALUE)
            for ((row, sum) <- sums) {
              vector.setQuick(row, sum)
            }
            new SequentialAccessSparseVector(vector)
          }
        }
      }

      case (op: WriteMatrix) => {
        val matrix = evaluate(op.matrix)
        matrix.toArray().foreach({ case (index, row) =>
          println(index + " " + row)
        })
      }

      case (op: WriteVector) => {
        val vector = evaluateAsVector(op.vector)
        println(vector)
      }

      case (op: scalar) => {
        op.value
      }

      case (op: WriteScalarRef) => {
        val scalar = evaluateAsScalar(op.scalar)
        println(scalar)
      }
    }

  }

  def evaluate(in: Executable): RDD[(Int, Vector)] = {
    execute(in).asInstanceOf[RDD[(Int, Vector)]]
  }

  def evaluateAsVector(in: io.ssc.gilbert2.Vector) = {
    execute(in).asInstanceOf[Vector]
  }

  def evaluateAsScalar(in: ScalarRef) = {
    execute(in).asInstanceOf[Double]
  }

}
