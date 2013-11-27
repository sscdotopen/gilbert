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

package io.ssc.gilbert

import io.ssc.gilbert.ScalarsOperation._
import io.ssc.gilbert.CellwiseOperation._
import io.ssc.gilbert.VectorwiseOperation._
import io.ssc.gilbert.ScalarOperation.ScalarOperation


case class LoadMatrix(path: String, numRows: Int, numColumns: Int) extends Matrix

case class ScalarMatrixTransformation(var scalar: ScalarRef, var matrix: Matrix, operation: ScalarsOperation)
    extends Matrix

case class Transpose(var matrix: Matrix) extends Matrix

case class MatrixMult(var left: Matrix, var right: Matrix) extends Matrix

case class CellwiseMatrixMatrixTransformation(var left: Matrix, var right: Matrix, operation: CellwiseOperation)
    extends Matrix

case class CellwiseMatrixTransformation(var matrix: Matrix, operation: ScalarOperation) extends Matrix

case class VectorwiseMatrixTransformation(var matrix: Matrix, operation: VectorwiseOperation) extends Matrix

case class WriteMatrix(var matrix: Matrix) extends Matrix

case class ones(rows: Int, columns: Int) extends Matrix

///TODO change to sprandn?
case class rand(rows: Int, columns: Int, mean: Double = 0, std: Double = 1) extends Matrix

case class FixpointIteration(initialState: Matrix, updateFunction: Matrix => Matrix) extends Matrix {
  val updatePlan = updateFunction.apply(IterationStatePlaceholder())
}

case class IterationStatePlaceholder() extends Matrix

case class scalar(value: Double) extends ScalarRef

case class AggregateMatrixTransformation(var matrix: Matrix, operation: ScalarsOperation) extends ScalarRef

case class ScalarScalarTransformation(var left: ScalarRef, var right: ScalarRef, operation: ScalarsOperation)
    extends ScalarRef

case class WriteScalarRef(var scalar: ScalarRef) extends ScalarRef