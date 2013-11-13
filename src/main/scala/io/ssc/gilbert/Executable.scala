package io.ssc.gilbert

trait Executable {}

trait Executor {

  private var executionOrder: Int = 0
  private var symbolTable = Map[Int, Any]()
  private var redirects = Map[Int, Int]()

  def setRedirects(redirects: Map[Int, Int]) = {
    this.redirects = redirects
  }

  def run(executable: Executable): Any

  protected def execute(transformation: Executable): Any

  def evaluate[T](in: Executable) = {
    execute(in).asInstanceOf[T]
  }

  def handle[T <: Executable, I](executable: T, retrieveInput: (T) => I, handle: (T, I) => Any): Any = {

    val input = retrieveInput(executable)

    executionOrder += 1

    /* check if this a common subexpression which we already processed */
   // if (redirects.contains(executionOrder)) {
    //   return symbolTable(redirects(executionOrder))
    //}

    println(executionOrder + " " + executable)

    val output = handle(executable, input)

    symbolTable += (executionOrder -> output)

    output
  }
}
