package io.ssc.gilbert.optimization

import io.ssc.gilbert.{IterationStatePlaceholder, Executable}

class IterationStateDetector(val transformation: Executable) extends Walker {

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
