package io.ssc.gilbert2

//TODO somehow doesn't work
object GilbertImplicits {

  implicit def Int2Scalar(value: Int) = scalar(value.toDouble)
  implicit def Double2Scalar(value: Int) = scalar(value)
}