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

package org.gilbertlang.shell

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.ILoop

object GilbertShellRunner extends App {

  val settings = new Settings
  settings.usejavacp.value = true
  settings.deprecation.value = true

  new GilbertShell().process(settings)
}

class GilbertShell extends ILoop {

  override def prompt = "> "

  addThunk {
    intp.beQuietDuring {
      intp.addImports("org.gilbertlang._")
      intp.addImports("org.gilbertlang.runtime._")
      intp.addImports("org.gilbertlang.runtime.reference._")
      intp.addImports("org.gilbertlang.shell._")
      intp.addImports("org.gilbertlang.operations._")
    }
  }

  override def printWelcome() {
    echo(
      """
               _ _ _               _
              (_) | |             | |
          __ _ _| | |__   ___ _ __| |_
         / _` | | | '_ \ / _ \ '__| __|
        | (_| | | | |_) |  __/ |  | |_
         \__, |_|_|_.__/ \___|_|   \__|
          __/ |
         |___/  Distributed Linear Algebra on Sparse Matrices

      """)
  }

}