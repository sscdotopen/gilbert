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

package org.gilbertlang.optimizer

import org.gilbertlang.runtime.Executable

case class CommonSubexpressionCandidate(val rootId: Int, val iterationId: Option[Int],
                                        val containsIterationState: Boolean)

//TODO needs to handle iteration state!
class CommonSubexpressionDetector extends DagWalker {

  private var candidatesByHash = Map[Int, Seq[CommonSubexpressionCandidate]]()

  def find(executable: Executable) = {
    visit(executable)

    var repeatedExpressions = Map[Int,Int]()

    for ((hash, candidates) <- candidatesByHash) {
      if (candidates.size > 1) {

        candidates.foreach(println)
        println("----------------")
        //val minOrder = candidates.reduce(math.min)
        //val toEliminate = candidates.filter(_ != minOrder)

        //toEliminate.map((_ -> minOrder)).foreach(repeatedExpressions += _)
      }

      //TODO check object equality to handle hash collisions!
    }

    repeatedExpressions
  }

  override def onLeave(transformation: Executable) = {

    //TODO merge, we shouldn't traverse the subtree twice
    val hash = transformation.hashCode()
    val containsIterationState = new IterationStateDetector(transformation).containsIterationState()
    val iterationId = currentIteration()
    
    //println("\t" + transformation.id + ", " + currentIteration().getOrElse("-") + " " + containsIterationState + " " + transformation)

    val candidate = CommonSubexpressionCandidate(transformation.id, iterationId, containsIterationState)

    val candidates = candidatesByHash.getOrElse(hash, Seq()) ++ Seq(candidate)
    candidatesByHash += (hash -> candidates)
  }
}
