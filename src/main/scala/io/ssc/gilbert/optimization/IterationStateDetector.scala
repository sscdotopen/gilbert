package io.ssc.gilbert.optimization

import io.ssc.gilbert.{IterationStatePlaceholder, Executable}

object IterationStateDetector extends Walker {

  var found = false

  override def onLeave(transformation: Executable) = {

    transformation match {
      case IterationStatePlaceholder => true
      case _ => false
    }
  }

}
