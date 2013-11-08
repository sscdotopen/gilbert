/**
 * gilbert - Distributed Linear Algebra on Sparse Matrices
 * Copyright (C) 2013  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.gilbert2


object Examples {

  def cooccurrences = {

    val A = load("/A")

    val B = bin(A)
    val C = B.t * B

    val D = C / C.max()

    WriteMatrix(D)
  }

  def pageRank = {

    val A = LoadMatrix("/A")
    val p = LoadVector("/p")
    val e = LoadVector("/e")
    val beta = scalar(0.85)
    val one = scalar(1)

    //TODO start loop
    val p_next = beta * A * p + (one - beta) * e
    //TODO end loop

    p_next
  }

  def linearRegression = {

    val minusOne = scalar(-1)

    val V = LoadMatrix("/X")
    val y = LoadVector("/y")

    val w = LoadVector("/w")
    val lambda = scalar(0.000001)

    // r = -V' * y
    val r = minusOne.times(V.transpose().times(y))
    // p = -r
    val p = minusOne.times(r)

    // norm_r2 = sum(r * r)
    val norm_r2 = r.norm2Squared()

    //TODO start loop

    // q = V' * V * p + lambda * p
    val q = V.transpose().times(V).times(p).plus(lambda.times(p))

    // alpha = norm_2 / p'*q
    val alpha = norm_r2.div(p.dot(q))

    // w = w + alpha * p
    val w_next = w.plus(alpha.times(p))

    // old_norm_r2=norm_r2;
    //val old_norm_r2 = norm_r2

    // r = r + alpha * q
    //val r_next = r.plus(alpha.times(q))

    // norm_r2 = sum(r * r)
    //val norm_r2_next = r_next.norm2Squared()

    // beta=norm_r2/old_norm_r2;
    //val beta = norm_r2.div(old_norm_r2)

    // p=-r+beta * p;
    //val p_next = minusOne.times(r.plus(beta.times(p)))

    //TODO end loop

    w_next
  }

}
