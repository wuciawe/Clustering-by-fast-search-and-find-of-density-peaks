package generic

import java.io.{FileWriter, PrintWriter}

import resource._

/**
 * Created by jwhu on 14-8-25.
 * At 下午6:29
 */
object Test {

  class myItem(val x: Double, val y: Double) extends ComputableItem[myItem] {

    @inline
    override def computeDistance(that: myItem): Double = math.sqrt(math.pow(this.x - that.x, 2) + math.pow(this.y - that.y, 2))

    @inline
    override def getString(): String = s"$x,$y"
  }

  def computeRhoDelta(threshold: Double, fileName: String, flag: Boolean = false): Unit = {
    var input = List.empty[myItem]
    for {
      managedFile <- managed(io.Source.fromFile(fileName))
      line <- managedFile.getLines()
      if line.size > 0
    } {
      val mm = line.split("\t").take(2).map(_.toDouble)
      input = new myItem(mm.head, mm.last) :: input
    }

    val fc = new FastCluster[myItem]()
    val RhoDelta = fc.computeRhoDelta(input.toList, threshold, flag)
    val pw = new PrintWriter(new FileWriter("rhodelta", false))
    pw.println(RhoDelta.map(x => s"${x._1} ${x._2} ${x._3}").mkString("\n"))
    pw.close()
  }

  def cluster(threshold: Double, fn: (Int, Double) => Boolean): Unit = {
    var input = List.empty[(myItem, Int, Double)]
    for {
      managedFile <- managed(io.Source.fromFile("rhodelta"))
      line <- managedFile.getLines()
      if line.size > 0
    } {
      val mm = line.split(" ")
      val mmm = mm.head.split(",")
      input = (new myItem(mmm.head.toDouble, mmm.last.toDouble), mm.tail.head.toInt, mm.last.toDouble) :: input
    }

    val fc = new FastCluster[myItem]()
    val clusters = fc.cluster(input, threshold, fn)
    val pw = new PrintWriter(new FileWriter("result", false))
    pw.println(clusters.mkString("\n"))
    pw.close()
  }

  def main(args: Array[String]) {

//    computeRhoDelta(2.5, "spiral.txt")
//    cluster(2.5, (rho: Int, delta: Double) => rho > 15 && delta > 5)

    computeRhoDelta(2, "Aggregation.txt")
    cluster(6, (rho: Int, delta: Double) => delta > 6)

//    computeRhoDelta(2.5, "D31.txt")
//    cluster(2, (rho: Int, delta: Double) => rho > 1 && delta > 2)
  }

}
