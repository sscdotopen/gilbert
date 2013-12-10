package org.gilbertlang.optimizer

import org.gilbertlang.operations.IterationStatePlaceholder
import org.gilbertlang.runtime.Executable

class IterationStateDetector(val transformation: Executable) extends DagWalker {

  var found = false

  def containsIterationState() = {
    visit(transformation)
    found
  }

  override def onLeave(transformation: Executable) = {

    transformation match {
      case _: IterationStatePlaceholder => found = true
      case _ =>
    }
  }

}
