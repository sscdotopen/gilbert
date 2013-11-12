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

import io.ssc.gilbert2.Executable


class CommonSubexpressionDetector extends Walker {

  private var subtreesByHash = Map[Int, Seq[Int]]()
  private var executionOrder = 0

  def find(executable: Executable) = {
    visit(executable)

    var redirects = Map[Int,Int]()

    for ((hash, orders) <- subtreesByHash) {
      if (orders.size > 1) {
        val minOrder = orders.reduce(math.min)
        val toEliminate = orders.filter(_ != minOrder)

        toEliminate.map((_ -> minOrder)).foreach(redirects += _)
        //eliminatedExpressions.foreach(redirects += (_ -> minOrder))
      }
    }

    redirects
  }

  override def onLeave(transformation: Executable) = {
    executionOrder += 1
    val hash = transformation.hashCode()

    //println("\t" + executionOrder + " " + transformation)

    val orders = subtreesByHash.getOrElse(hash, Seq()) ++ Seq(executionOrder)
    subtreesByHash += (hash -> orders)
  }
}
