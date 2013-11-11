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

object ScalarOperation extends Enumeration {
  type ScalarOperation = Value
  val Binarize = Value
}

object ScalarsOperation extends Enumeration {
  type ScalarsOperation = Value
  val Addition, Subtraction, Multiplication, Division, Maximum = Value
}

object CellwiseOperation extends Enumeration {
  type CellwiseOperation = Value
  val Addition, Subtraction, Multiplication, Division = Value
}

object VectorwiseOperation extends Enumeration {
  type VectorwiseOperation = Value
  val Max, Min, Average, Norm2Squared = Value
}

object MatrixwiseOperation extends Enumeration {
  type MatrixwiseOperation = Value
  val RowSums = Value
}