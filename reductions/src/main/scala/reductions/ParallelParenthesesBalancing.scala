package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def checkCorrectness(chars: Array[Char], currentBal:Int): Boolean = {
      if (currentBal < 0)
        false
      else if (chars.length == 0)
        currentBal == 0
      else {
        val newBal = chars.head match {
          case '(' => currentBal + 1
          case ')' => currentBal - 1
          case _ => currentBal
        }
        checkCorrectness(chars.tail, newBal)
      }
    }
    checkCorrectness(chars, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, leftOpened: Int, leftClosed: Int): (Int, Int) = {
      if (idx < until) {
        chars(idx) match {
          case '(' => traverse(idx + 1, until, leftOpened + 1, leftClosed)
          case ')' =>
            if (leftOpened > 0)
              traverse(idx + 1, until, leftOpened - 1, leftClosed)
            else
              traverse(idx + 1, until, leftOpened, leftClosed + 1)
          case _ => traverse(idx + 1, until, leftOpened, leftClosed)
        }
      } else
        (leftOpened, leftClosed)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      val length = until - from
      if (length > threshold) {
        val middle = from + length / 2
        val ((leftOpened1, leftClosed1), (leftOpened2, leftClosed2)) =
          parallel(reduce(from, middle), reduce(middle, until))
        if (leftOpened1 >= leftClosed2)
          (leftOpened1 - leftClosed2 + leftOpened2, leftClosed1)
        else
          (leftOpened2, leftClosed1 - leftOpened1 + leftClosed2)
      } else
        traverse(from, until, 0, 0)
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
