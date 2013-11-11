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

object load {
  def apply(path: String) = LoadMatrix(path)
}

object bin {
  def apply(matrix: Matrix) = matrix.binarize()
}

object max {
  def apply(matrix: Matrix) = matrix.max()
  def apply(vector: Vector) = vector.max()
}

object min {
  def apply(vector: Vector) = vector.min()
}

object avg {
  def apply(vector: Vector) = vector.avg()
}

object rowSum {
  def apply(matrix: Matrix) = matrix.rowSum()
}
