package io.ssc.gilbert2

trait Executable {}

trait Executor {
  def run(executable: Executable): Any
}
