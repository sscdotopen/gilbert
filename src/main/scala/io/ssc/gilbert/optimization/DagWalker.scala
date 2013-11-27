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

package io.ssc.gilbert.optimization

import io.ssc.gilbert._
import io.ssc.gilbert.MatrixMult
import io.ssc.gilbert.WriteMatrix
import io.ssc.gilbert.Transpose
import io.ssc.gilbert.ones
import io.ssc.gilbert.CellwiseMatrixTransformation
import io.ssc.gilbert.rand
import io.ssc.gilbert.LoadMatrix
import io.ssc.gilbert.AggregateMatrixTransformation
import io.ssc.gilbert.scalar
import io.ssc.gilbert.ScalarMatrixTransformation
import scala.collection.mutable.HashSet

abstract class DagWalker {

  private var iteration: Option[Int] = None
  private val visited = HashSet.empty[Int]

  def currentIteration() = iteration

  def onArrival(transformation: Executable) = {}
  def onLeave(transformation: Executable) = {}

  def visit(transformation: Executable): Unit = {

    val firstVisit = visited.add(transformation.id)

    if (!firstVisit) {
      return
    }

    transformation match {

      case transformation: LoadMatrix => {
        onArrival(transformation)
        onLeave(transformation)
      }

      //TODO needs to handle nested iterations?
      case transformation: FixpointIteration => {
        iteration = Some(transformation.id)

        onArrival(transformation)
        visit(transformation.initialState)
        visit(transformation.updatePlan)
        onLeave(transformation)

        iteration = None
      }

      case transformation: IterationStatePlaceholder => {
        onArrival(transformation)
        onLeave(transformation)
      }

      case transformation: CellwiseMatrixTransformation => {
        onArrival(transformation)
        visit(transformation.matrix)
        onLeave(transformation)
      }

      case transformation: CellwiseMatrixMatrixTransformation => {
        onArrival(transformation)
        visit(transformation.left)
        visit(transformation.right)
        onLeave(transformation)
      }

      case transformation: Transpose => {
        onArrival(transformation)
        visit(transformation.matrix)
        onLeave(transformation)
      }

      case transformation: MatrixMult => {
        onArrival(transformation)
        visit(transformation.left)
        visit(transformation.right)
        onLeave(transformation)
      }

      case transformation: AggregateMatrixTransformation => {
        onArrival(transformation)
        visit(transformation.matrix)
        onLeave(transformation)
      }

      case transformation: ScalarMatrixTransformation => {
        onArrival(transformation)
        visit(transformation.matrix)
        visit(transformation.scalar)
        onLeave(transformation)
      }

      case transformation: VectorwiseMatrixTransformation => {
        onArrival(transformation)
        visit(transformation.matrix)
        onLeave(transformation)
      }

      case transformation: ones => {
        onArrival(transformation)
        onLeave(transformation)
      }

      case transformation: rand => {
        onArrival(transformation)
        onLeave(transformation)
      }

      case transformation: WriteMatrix => {
        onArrival(transformation)
        visit(transformation.matrix)
        onLeave(transformation)
      }

      case transformation: scalar => {
        onArrival(transformation)
        onLeave(transformation)
      }

      case transformation: WriteScalarRef => {
        onArrival(transformation)
        visit(transformation.scalar)
        onLeave(transformation)
      }
    }
  }

}
