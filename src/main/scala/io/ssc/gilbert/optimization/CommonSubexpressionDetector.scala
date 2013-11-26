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

import io.ssc.gilbert.Executable

case class Subexpression(val rootId: Int, val iterationId: Option[Int], val containsIterationState: Boolean)

//TODO needs to handle iteration state!
class CommonSubexpressionDetector extends Walker {

  private var subtreesByHash = Map[Int, Seq[Int]]()

  def find(executable: Executable) = {
    visit(executable)

    var repeatedExpressions = Map[Int,Int]()

    for ((hash, orders) <- subtreesByHash) {
      if (orders.size > 1) {
        val minOrder = orders.reduce(math.min)
        val toEliminate = orders.filter(_ != minOrder)

        toEliminate.map((_ -> minOrder)).foreach(repeatedExpressions += _)
        //eliminatedExpressions.foreach(repeatedExpressions += (_ -> minOrder))
      }

      //TODO check object equality to handle hash collisions!
    }

    repeatedExpressions
  }

  override def onLeave(transformation: Executable) = {

    val hash = transformation.hashCode()

    val containsIterationState = new IterationStateDetector(transformation).containsIterationState()

    println("\t" + transformation.id + ", " + currentIteration().getOrElse("-") + " " + containsIterationState + " " + transformation)



    val orders = subtreesByHash.getOrElse(hash, Seq()) ++ Seq(transformation.id)
    subtreesByHash += (hash -> orders)
  }
}
