package org.gilbertlang.optimizer

object PlayMC {

  def main(args: Array[String]) = {
    dp(Array(20, 100, 20, 1))
  }

  def dp(dimensions: Array[Int]) = {
    val n = dimensions.length - 1
    val costs = Array.ofDim[Int](n, n)
    val s = Array.ofDim[Int](n, n)

    for (ii <- 1 until n) {

      for (i <- 0 until (n - ii)) {

        val j = i + ii
        costs(i)(j) = Int.MaxValue
        println("\tj " + j)

        for (k <- i until j) {

          println("\t\tk " + k)

          val q = costs(i)(k) + costs(k + 1)(j) + dimensions(i) * dimensions(k + 1) * dimensions(j + 1)

          if (q < costs(i)(j)) {
            costs(i)(j) = q
            s(i)(j) = k
          }
        }
      }
    }

    for (v <- 0 until n) {

      for (w <- 0 until n) {
        print(costs(v)(w) + ", ")
      }

      println()
    }

    println()

    for (v <- 0 until n) {

      for (w <- 0 until n) {
        print(s(v)(w) + ", ")
      }

      println()
    }

  }
}

