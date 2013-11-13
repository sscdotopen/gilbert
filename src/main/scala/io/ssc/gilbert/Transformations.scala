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
import io.ssc.gilbert.MatrixwiseOperation.MatrixwiseOperation


case class LoadMatrix(path: String, numRows: Int, numColumns: Int) extends Matrix

case class ScalarMatrixTransformation(scalar: ScalarRef, matrix: Matrix, operation: ScalarsOperation)
    extends Matrix

case class Transpose(matrix: Matrix) extends Matrix

case class MatrixMult(left: Matrix, right: Matrix) extends Matrix

case class CellwiseMatrixMatrixTransformation(left: Matrix, right: Matrix, operation: CellwiseOperation)
    extends Matrix

case class CellwiseMatrixTransformation(matrix: Matrix, operation: ScalarOperation) extends Matrix

case class VectorwiseMatrixTransformation(matrix: Matrix, operation: VectorwiseOperation) extends Matrix

case class WriteMatrix(matrix: Matrix) extends Matrix



case class LoadVector(path: String) extends Vector

case class ScalarVectorTransformation(scalar: ScalarRef, vector: Vector, operation: ScalarsOperation) 
    extends Vector

case class MatrixVectorMult(matrix: Matrix, vector: Vector) 
    extends Vector

case class CellwiseVectorTransformation(left: Vector, right: Vector, operation: CellwiseOperation)
    extends Vector

case class MatrixToVectorTransformation(matrix: Matrix, operation: MatrixwiseOperation)
    extends Vector

case class WriteVector(vector: Vector) extends Vector

case class ones(size: Int) extends Vector

case class rand(size: Int, mean: Double = 0, std: Double = 1) extends Vector

case class FixpointIteration(initialState: Vector, updateFunction: Vector => Vector) extends Vector {
  val updatePlan = updateFunction.apply(IterationStatePlaceholder())
}

case class IterationStatePlaceholder() extends Vector



case class scalar(value: Double) extends ScalarRef

case class VectorAggregationTransformation(vector: Vector, operation: VectorwiseOperation) extends ScalarRef

case class DotProductTransformation(left: Vector, right: Vector) extends ScalarRef

case class AggregateMatrixTransformation(matrix: Matrix, operation: ScalarsOperation) extends ScalarRef

case class ScalarScalarTransformation(left: ScalarRef, right: ScalarRef, operation: ScalarsOperation) extends ScalarRef

case class WriteScalarRef(scalar: ScalarRef) extends ScalarRef