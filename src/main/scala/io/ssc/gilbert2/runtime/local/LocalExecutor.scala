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
import org.apache.mahout.math.function.{VectorFunction, DoubleDoubleFunction, DoubleFunction, Functions}
import io.ssc.gilbert2.runtime.VectorFunctions
import io.ssc.gilbert2.AggregateMatrixTransformation
import io.ssc.gilbert2.MatrixMult
import io.ssc.gilbert2.CellwiseMatrixTransformation
import io.ssc.gilbert2.WriteMatrix
import io.ssc.gilbert2.Transpose
import io.ssc.gilbert2.LoadMatrix
import io.ssc.gilbert2.ScalarMatrixTransformation
import scala.math
import org.apache.mahout.math.random.Normal
import io.ssc.gilbert2.optimization.CommonSubexpressionDetector
import io.ssc.gilbert2.shell.printPlan


object LocalExecutorRunner {

  def main(args: Array[String]): Unit = {
    val A = LoadMatrix("/A")

    val B = A.binarize()

    val C = B.t * B

    new LocalExecutor().run(WriteMatrix(C))
  }
}

class LocalExecutor extends Executor {

  var executionOrder = 0
  var symbolTable = Map[Int,Any]()

  //TODO ugly, refactor
  var redirects = Map[Int,Int]()

  def run(executable: Executable) = {

    redirects = new CommonSubexpressionDetector().find(executable)

    printPlan(executable)
    println("common subexpressions " + redirects)

    execute(executable)
  }

  //TODO refactor
  protected def execute(executable: Executable): Any = {

    //println("Executing " + executable)

    executable match {
      case (op: LoadMatrix) => {

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        val matrix = new SparseRowMatrix(3, 3)
        matrix.setQuick(0, 0, 3)
        matrix.setQuick(0, 2, 1)
        matrix.setQuick(1, 0, 7)
        matrix.setQuick(1, 1, 8)
        matrix.setQuick(2, 0, 3)
        matrix.setQuick(2, 1, 9)
        matrix.setQuick(2, 2, 8)

        symbolTable += (executionOrder -> matrix)

        matrix
      }

      case (op: CellwiseMatrixTransformation) => {

        val matrix = evaluate(op.matrix)

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        op.operation match {
          case ScalarOperation.Binarize => {
            matrix.assign(VectorFunctions.binarize)
          }
        }

        symbolTable += (executionOrder -> matrix)

        matrix
      }

      case (op: Transpose) => {

        val matrix = evaluate(op.matrix)

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        val transposedMatrix = matrix.transpose()

        symbolTable += (executionOrder -> transposedMatrix)

        transposedMatrix
      }

      case (op: MatrixMult) => {

        val leftMatrix = evaluate(op.left)
        val rightMatrix = evaluate(op.right)

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        val resultMatrix = leftMatrix.times(rightMatrix)

        symbolTable += (executionOrder -> resultMatrix)

        resultMatrix
      }

      case (op: AggregateMatrixTransformation) => {

        val matrix = evaluate(op.matrix)

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        val result = op.operation match {
          case (ScalarsOperation.Maximum) => { matrix.aggregate(max, identity) }
        }

        symbolTable += (executionOrder -> result)

        result
      }

      case (op: ScalarMatrixTransformation) => {

        val matrix = evaluate(op.matrix)
        val scalar = evaluateAsScalar(op.scalar)

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        val resultMatrix = op.operation match {
          case (ScalarsOperation.Division) => {
            matrix.divide(scalar)
          }
        }

        symbolTable += (executionOrder -> resultMatrix)

        resultMatrix
      }

      case (op: MatrixToVectorTransformation) => {

        val matrix = evaluate(op.matrix)

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        val vector = op.operation match {
          case (MatrixwiseOperation.RowSums) => {
            matrix.aggregateRows(new VectorFunction {
              def apply(v: Vector) = v.zSum()
            })
          }
        }

        symbolTable += (executionOrder -> vector)

        vector
      }

      case (op: ScalarVectorTransformation) => {

        val vector = evaluateAsVector(op.vector)
        val scalar = evaluateAsScalar(op.scalar)

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        val resultVector = op.operation match {
          case (ScalarsOperation.Division) => {
            vector.assign(Functions.DIV, scalar)
          }
        }

        symbolTable += (executionOrder -> resultVector)

        resultVector
      }

      case (op: ones) => {

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        val vector = new DenseVector(op.size).assign(1)

        symbolTable += (executionOrder -> vector)

        vector
      }

      case (op: rand) => {

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        val vector = new DenseVector(op.size).assign(new Normal(op.mean, op.std))

        symbolTable += (executionOrder -> vector)

        vector
      }

      case (op: WriteMatrix) => {

        val matrix = evaluate(op.matrix)

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        symbolTable += (executionOrder -> matrix)

        println(matrix)
      }

      case (op: WriteVector) => {

        val vector = evaluateAsVector(op.vector)

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        symbolTable += (executionOrder -> vector)

        println(vector)
      }

      case (op: scalar) => {

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        symbolTable += (executionOrder -> op.value)

        op.value
      }

      case (op: WriteScalarRef) => {

        val scalar = evaluateAsScalar(op.scalar)

        executionOrder += 1

        if (redirects.contains(executionOrder)) {
          return symbolTable(redirects(executionOrder))
        }

        println(executionOrder + " " + op)

        symbolTable += (executionOrder -> scalar)

        println(scalar)
      }        
    }

  }

  def evaluate(in: Executable) = {
    execute(in).asInstanceOf[SparseRowMatrix]
  }

  def evaluateAsVector(in: io.ssc.gilbert2.Vector) = {
    execute(in).asInstanceOf[Vector]
  }

  def evaluateAsScalar(in: ScalarRef) = {
    execute(in).asInstanceOf[Double]
  }

  def max = new DoubleDoubleFunction {
    def apply(p1: Double, p2: Double) = { math.max(p1, p2) }
  }

  def identity = new DoubleFunction {
    def apply(p1: Double) = { p1 }
  }
}

