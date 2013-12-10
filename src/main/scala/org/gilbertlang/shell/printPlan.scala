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

package org.gilbertlang.shell

import org.gilbertlang.operations.{WriteScalarRef, IterationStatePlaceholder, FixpointIteration, VectorwiseMatrixTransformation, CellwiseMatrixMatrixTransformation, ScalarMatrixTransformation, AggregateMatrixTransformation, scalar, rand, ones, WriteMatrix, CellwiseMatrixTransformation, MatrixMult, Transpose, LoadMatrix}
import org.gilbertlang.runtime.Executable

object printPlan {

  def apply(executable: Executable) = {
    new PlanPrinter().print(executable)
  }
}

class PlanPrinter {

  def print(executable: Executable, depth: Int = 0): Unit = {

    executable match {

      case transformation: LoadMatrix => {
        printIndented(depth, transformation, "LoadMatrix [" + transformation.path + "]")
      }

      case transformation: FixpointIteration => {
        printIndented(depth, transformation, "FixpointIteration")
        print(transformation.initialState, depth + 1)
        print(transformation.updatePlan, depth + 1)
      }

      case transformation: IterationStatePlaceholder => {
        printIndented(depth, transformation, "IterationState")
      }

      case transformation: CellwiseMatrixTransformation => {
        printIndented(depth, transformation, "CellwiseMatrixOp [" + transformation.operation + "]")
        print(transformation.matrix, depth + 1)
      }

      case transformation: CellwiseMatrixMatrixTransformation => {
        printIndented(depth, transformation, "CellwiseMatrixMatrixTransformation [" + transformation.operation + "]")
        print(transformation.left, depth + 1)
        print(transformation.right, depth + 1)
      }

      case transformation: Transpose => {
        printIndented(depth, transformation, "Transpose")
        print(transformation.matrix, depth + 1)
      }

      case transformation: MatrixMult => {
        printIndented(depth, transformation, "MatrixMult")
        print(transformation.left, depth + 1)
        print(transformation.right, depth + 1)
      }

      case transformation: AggregateMatrixTransformation => {
        printIndented(depth, transformation, "AggregateMatrixOp [" + transformation.operation + "]")
        print(transformation.matrix, depth + 1)
      }

      case transformation: VectorwiseMatrixTransformation => {
        printIndented(depth, transformation, "VectorwiseMatrixTransformation [" + transformation.operation + "]")
        print(transformation.matrix, depth + 1)
      }

      case transformation: ScalarMatrixTransformation => {
        printIndented(depth, transformation, "ScalarMatrixOp [" + transformation.operation + "]")
        print(transformation.matrix, depth + 1)
        print(transformation.scalar, depth + 1)
      }

      case transformation: ones => {
        printIndented(depth, transformation, transformation.toString)
      }

      case transformation: rand => {
        printIndented(depth, transformation, transformation.toString)
      }

      case transformation: WriteMatrix => {
        printIndented(depth, transformation, "WriteMatrix")
        print(transformation.matrix, depth + 1)
      }

      case transformation: scalar => {
        printIndented(depth, transformation, transformation.value.toString)
      }

      case transformation: WriteScalarRef => {
        printIndented(depth, transformation, "WriteScalarRef")
        print(transformation.scalar, depth + 1)
      }
    }

  }

  def printIndented(depth: Int, transformation: Executable, str: String) = {
    println("".padTo(depth, "  ").mkString + "(" + transformation.id + ") " + str)
  }
}
