/*
 * Copyright (C) 2012 The Regents of The University California. 
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.ssc.gilbert2.runtime.spark

import java.nio.ByteBuffer
import org.apache.spark.serializer.{KryoSerializer => SparkKryoSerializer}

/**
 * A wrapper around some unserializable objects that make them both Java
 * serializable. Internally, Kryo is used for serialization.
 *
 * Use KryoSerializationWrapper(value) to create a wrapper.
 */
class KryoSerializationWrapper[T] extends Serializable {

  @transient var value: T = _

  private var valueSerialized: Array[Byte] = _

  private def writeObject(out: java.io.ObjectOutputStream) {
    valueSerialized = KryoSerializer.serialize(value)
    out.defaultWriteObject()
  }

  private def readObject(in: java.io.ObjectInputStream) {
    in.defaultReadObject()
    value = KryoSerializer.deserialize[T](valueSerialized)
  }
}

/**
 * Java object serialization using Kryo. This is much more efficient, but Kryo
 * sometimes is buggy to use. We use this mainly to serialize the object
 * inspectors.
 */
object KryoSerializer {

  @transient val ser = new SparkKryoSerializer

  def serialize[T](o: T): Array[Byte] = {
    ser.newInstance().serialize(o).array()
  }

  def deserialize[T](bytes: Array[Byte]): T  = {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
  }
}

object KryoSerializationWrapper {
  def apply[T](value: T): KryoSerializationWrapper[T] = {
    val wrapper = new KryoSerializationWrapper[T]
    wrapper.value = value
    wrapper
  }
}
