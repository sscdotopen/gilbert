package org

import org.gilbertlang.operations.scalar

package object gilbertlang {
  implicit def Int2Scalar(value: Int) = scalar(value.toDouble)
  implicit def Double2Scalar(value: Double) = scalar(value)
}
