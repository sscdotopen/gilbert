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

package io.ssc.gilbert2.runtime

import org.apache.mahout.math.function.{DoubleDoubleFunction, VectorFunction, DoubleFunction}
import org.apache.mahout.math.Vector

object VectorFunctions {

  def binarize = new DoubleFunction {
    def apply(v: Double) = {
      if (v == 0) { 0 } else { 1 }
    }
  }

  def sum = new VectorFunction {
    def apply(v: Vector) = v.zSum()
  }

  def max = new DoubleDoubleFunction {
    def apply(p1: Double, p2: Double) = { math.max(p1, p2) }
  }

  def identity = new DoubleFunction {
    def apply(p1: Double) = { p1 }
  }
}
