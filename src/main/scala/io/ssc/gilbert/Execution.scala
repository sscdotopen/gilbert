package io.ssc.gilbert

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

object IDGenerator {

  val counter = new AtomicInteger(0)

  def nextID() = counter.incrementAndGet()
}


abstract class Executable() {
  val id: Int = IDGenerator.nextID()
}

trait Executor {

  private val symbolTable = mutable.Map[Int, Any]()

  def run(executable: Executable): Any


  protected def execute(transformation: Executable): Any


  def evaluate[T](in: Executable) = execute(in).asInstanceOf[T]


  def handle[T <: Executable, I](executable: T, retrieveInput: (T) => I, handle: (T, I) => Any): Any = {

    val input = retrieveInput(executable)

    /* check if we already processed this expression */
    if (symbolTable.contains(executable.id)) {
      println("\t reusing (" + executable.id + ")")
      return symbolTable(executable.id)
    }

    println("\t executing (" + executable.id + ") " + executable)

    val output = handle(executable, input)

    symbolTable.update(executable.id, output)

    output
  }
}
