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

package io.ssc.gilbert2.shell

import io.ssc.gilbert2._
import io.ssc.gilbert2.AggregateMatrixTransformation
import io.ssc.gilbert2.WriteVector
import io.ssc.gilbert2.MatrixMult
import io.ssc.gilbert2.scalar
import io.ssc.gilbert2.CellwiseMatrixTransformation
import io.ssc.gilbert2.ScalarVectorTransformation
import io.ssc.gilbert2.WriteMatrix
import io.ssc.gilbert2.Transpose
import io.ssc.gilbert2.ones
import io.ssc.gilbert2.rand
import io.ssc.gilbert2.LoadMatrix
import io.ssc.gilbert2.MatrixToVectorTransformation

object printPlan {

  def main(args: Array[String]) = {
    printPlan(Examples.cooccurrences)
  }

  def apply(executable: Executable) = {
    new PlanPrinter().print(executable)
  }
}

class PlanPrinter {

  def print(executable: Executable, depth: Int = 0): Unit = {

    executable match {

      case (op: LoadMatrix) => {
        printIndented(depth, "LoadMatrix [" + op.path + "]")
      }

      case (op: CellwiseMatrixTransformation) => {
        printIndented(depth, "CellwiseMatrixOp [" + op.operation + "]")
        print(op.matrix, depth + 1)
      }

      case (op: Transpose) => {
        printIndented(depth, "Transpose")
        print(op.matrix, depth + 1)
      }

      case (op: MatrixMult) => {
        printIndented(depth, "MatrixMult")
        print(op.left, depth + 1)
        print(op.right, depth + 1)
      }

      case (op: AggregateMatrixTransformation) => {
        printIndented(depth, "AggregateMatrixOp [" + op.operation + "]")
        print(op.matrix, depth + 1)
      }

      case (op: ScalarMatrixTransformation) => {
        printIndented(depth, "ScalarMatrixOp [" + op.operation + "]")
        print(op.matrix, depth + 1)
        print(op.scalar, depth + 1)
      }

      case (op: MatrixToVectorTransformation) => {
        printIndented(depth, "MatrixToVectorAggregationOp [" + op.operation + "]")
        print(op.matrix, depth + 1)
      }

      case (op: ScalarVectorTransformation) => {
        printIndented(depth, "ScalarVectorOp [" + op.operation + "]")
        print(op.vector, depth + 1)
      }

      case (op: VectorAggregationTransformation) => {
        printIndented(depth, "VectorAggregationTransformation [" + op.operation + "]")
        print(op.vector, depth + 1)
      }

      case (op: ones) => {
        printIndented(depth, op.toString)
      }

      case (op: rand) => {
        printIndented(depth, op.toString)
      }

      case (op: WriteMatrix) => {
        printIndented(depth, "WriteMatrix")
        print(op.matrix, depth + 1)
      }

      case (op: WriteVector) => {
        printIndented(depth, "WriteVector")
        print(op.vector, depth + 1)
      }

      case (op: scalar) => {
        printIndented(depth, op.value.toString)
      }

      case (op: WriteScalarRef) => {
        printIndented(depth, "WriteScalarRef")
        print(op.scalar, depth + 1)
      }
    }

  }

  def printIndented(depth: Int, str: String) = {
    println("".padTo(depth, "  ").mkString + str)
  }
}
