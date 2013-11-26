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

abstract class Matrix extends Executable {

  def transpose() = { Transpose(this) }

  def *(other: Matrix) = { MatrixMult(this, other) }
  def *(scalar: ScalarRef) = { ScalarMatrixTransformation(scalar, this, ScalarsOperation.Multiplication) }

  def div(scalar: ScalarRef) = { ScalarMatrixTransformation(scalar, this, ScalarsOperation.Division) }
  def +(other: Matrix) = { CellwiseMatrixMatrixTransformation(this, other, CellwiseOperation.Addition) }
  def -(other: Matrix) = { CellwiseMatrixMatrixTransformation(this, other, CellwiseOperation.Subtraction) }
  def :*(other: Matrix) = { CellwiseMatrixMatrixTransformation(this, other, CellwiseOperation.Multiplication) }
  def :/(other: Matrix) = { CellwiseMatrixMatrixTransformation(this, other, CellwiseOperation.Division) }

  def binarize() = { CellwiseMatrixTransformation(this, ScalarOperation.Binarize) }

  def max() = { AggregateMatrixTransformation(this, ScalarsOperation.Maximum) }
  def norm(p: Int) = {
    p match {
      case 2 => AggregateMatrixTransformation(this, ScalarsOperation.Norm2)
    }
  }

  def t() = transpose

  def /(scalar: ScalarRef) = div(scalar)



  def normalizeRows(norm: Int) = {
    norm match {
      case 1 => VectorwiseMatrixTransformation(this, VectorwiseOperation.NormalizeL1)
    }
  }

  def sum(dimension: Int) = {
    dimension match {
      case 2 => VectorwiseMatrixTransformation(this, VectorwiseOperation.Sum)
    }
  }
}

abstract class ScalarRef extends Executable {

  def times(matrix: Matrix) = { ScalarMatrixTransformation(this, matrix, ScalarsOperation.Multiplication) }
  def div(matrix: Matrix) = { ScalarMatrixTransformation(this, matrix, ScalarsOperation.Division) }
  def div(other: ScalarRef) = { ScalarScalarTransformation(this, other, ScalarsOperation.Division) }
  def minus(other: ScalarRef) = { ScalarScalarTransformation(this, other, ScalarsOperation.Subtraction) }

  def *(matrix: Matrix) = times(matrix)
  def -(other: ScalarRef) = minus(other)
}



