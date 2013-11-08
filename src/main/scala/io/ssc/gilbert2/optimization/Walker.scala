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

package io.ssc.gilbert2.optimization

import io.ssc.gilbert2._
import io.ssc.gilbert2.WriteVector
import io.ssc.gilbert2.MatrixMult
import io.ssc.gilbert2.WriteMatrix
import io.ssc.gilbert2.Transpose
import io.ssc.gilbert2.ones
import io.ssc.gilbert2.ScalarVectorTransformation
import io.ssc.gilbert2.CellwiseMatrixTransformation
import io.ssc.gilbert2.rand
import io.ssc.gilbert2.LoadMatrix
import io.ssc.gilbert2.AggregateMatrixTransformation
import io.ssc.gilbert2.scalar
import io.ssc.gilbert2.ScalarMatrixTransformation
import io.ssc.gilbert2.MatrixToVectorTransformation

abstract class Walker {

  def onArrival(transformation: Executable) = {}
  def onLeave(transformation: Executable) = {}

  def visit(transformation: Executable): Unit = {

    transformation match {

      case (transformation: LoadMatrix) => {
        onArrival(transformation)
        onLeave(transformation)
      }

      case (transformation: CellwiseMatrixTransformation) => {
        onArrival(transformation)
        visit(transformation.matrix)
        onLeave(transformation)
      }

      case (transformation: Transpose) => {
        onArrival(transformation)
        visit(transformation.matrix)
        onLeave(transformation)
      }

      case (transformation: MatrixMult) => {
        onArrival(transformation)
        visit(transformation.left)
        visit(transformation.right)
        onLeave(transformation)
      }

      case (transformation: AggregateMatrixTransformation) => {
        onArrival(transformation)
        visit(transformation.matrix)
        onLeave(transformation)
      }

      case (transformation: ScalarMatrixTransformation) => {
        onArrival(transformation)
        visit(transformation.matrix)
        visit(transformation.scalar)
        onLeave(transformation)
      }

      case (transformation: MatrixToVectorTransformation) => {
        onArrival(transformation)
        visit(transformation.matrix)
        onLeave(transformation)
      }

      case (transformation: ScalarVectorTransformation) => {
        onArrival(transformation)
        visit(transformation.vector)
        onLeave(transformation)
      }

      case (transformation: ones) => {
        onArrival(transformation)
        onLeave(transformation)
      }

      case (transformation: rand) => {
        onArrival(transformation)
        onLeave(transformation)
      }

      case (transformation: WriteMatrix) => {
        onArrival(transformation)
        visit(transformation.matrix)
        onLeave(transformation)
      }

      case (transformation: WriteVector) => {
        onArrival(transformation)
        visit(transformation.vector)
        onLeave(transformation)
      }

      case (transformation: scalar) => {
        onArrival(transformation)
        onLeave(transformation)
      }

      case (transformation: WriteScalarRef) => {
        onArrival(transformation)
        visit(transformation.scalar)
        onLeave(transformation)
      }
    }
  }

}
