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

package io.ssc.gilbert2

abstract class Matrix extends Executable {

  def transpose() = { Transpose(this) }

  def times(other: Matrix) = { MatrixMult(this, other) }
  def times(scalar: ScalarRef) = { ScalarMatrixTransformation(scalar, this, ScalarsOperation.Multiplication) }
  def times(vector: Vector) = { MatrixVectorMult(this, vector) }

  def div(scalar: ScalarRef) = { ScalarMatrixTransformation(scalar, this, ScalarsOperation.Division) }

  def plus(other: Matrix) = { CellwiseMatrixMatrixTransformation(this, other, CellwiseOperation.Addition) }
  
  def binarize() = { CellwiseMatrixTransformation(this, ScalarOperation.Binarize) }

  def max() = { AggregateMatrixTransformation(this, ScalarsOperation.Maximum) }

  def t() = transpose
  def *(other: Matrix) = times(other)
  def *(scalar: ScalarRef) = times(scalar)
  def *(vector: Vector) = times(vector)

  def /(scalar: ScalarRef) = div(scalar)

  def rowSum() = { MatrixToVectorTransformation(this, MatrixwiseOperation.RowSums) }
}

abstract class Vector extends Executable {

  def plus(other: Vector) = { CellwiseVectorTransformation(this, other, CellwiseOperation.Addition) }
  def dot(other: Vector) = { DotProductTransformation(this, other) }

  def norm2Squared() = { VectorAggregationTransformation(this, VectorwiseOperation.Norm2Squared) }
  def max() = { VectorAggregationTransformation(this, VectorwiseOperation.Max) }
  def min() = { VectorAggregationTransformation(this, VectorwiseOperation.Min) }
  def avg() = { VectorAggregationTransformation(this, VectorwiseOperation.Average) }

  def +(other: Vector) = plus(other)
  def /(scalar: ScalarRef) = { ScalarVectorTransformation(scalar, this, ScalarsOperation.Division) }
}

abstract class ScalarRef extends Executable {
  def times(matrix: Matrix) = { ScalarMatrixTransformation(this, matrix, ScalarsOperation.Multiplication) }
  def times(vector: Vector) = { ScalarVectorTransformation(this, vector, ScalarsOperation.Multiplication) }
  def div(matrix: Matrix) = { ScalarMatrixTransformation(this, matrix, ScalarsOperation.Division) }
  def div(other: ScalarRef) = { ScalarScalarTransformation(this, other, ScalarsOperation.Division) }
  def minus(other: ScalarRef) = { ScalarScalarTransformation(this, other, ScalarsOperation.Subtraction) }

  def *(matrix: Matrix) = times(matrix)
  def *(vector: Vector) = times(vector)
  def -(other: ScalarRef) = minus(other)
}



