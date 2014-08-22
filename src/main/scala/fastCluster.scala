import java.io._
import java.util.Calendar

import akka.actor._
import akka.routing._
import resource._

import scala.collection.mutable

/**
 * Created by jwhu on 14-7-28.
 * At 下午9:54
 */

object fastCluster {

  val MN = Runtime.getRuntime.availableProcessors()
  val distance = 170000
  val threshold = 100
  val baseDir = "baseDir/"

  def writeList(list: List[String], des: String): Unit = {
    val desFile = new File(des)
    if (!desFile.getParentFile.exists()) desFile.getParentFile.mkdirs()
    val pw = new PrintWriter(new FileWriter(des, false))
    for (line <- list) {
      pw.println(line)
    }
    pw.close()
  }

  def cluster(): Unit = {
    val allPoints = new mutable.HashSet[Int]()
    val map = new mutable.OpenHashMap[Int, String]()
    for (file <- managed(io.Source.fromFile(s"${baseDir}gps2xy"))) {
      for (line <- file.getLines()) {
        val mm = line.split("\t")
        allPoints add mm.head.toInt
        map.put(mm.head.toInt, mm.tail.head)
      }
    }
    val cmap = new mutable.OpenHashMap[Int, mutable.HashSet[Int]]()
    val bmap = new mutable.OpenHashMap[Int, mutable.HashSet[Int]]()
    for (managedFile <- managed(io.Source.fromFile(s"${baseDir}${threshold}delta"))) {
      for (line <- managedFile.getLines()) {
        val mm = line.split("\t")
        val rho = mm.tail.head.toInt
        val delta = mm.last.toDouble
        if ((rho < 50 && delta > 3500 - 20 * rho) || (rho >= 50 && delta > 2500)) {
          val set = new mutable.HashSet[Int]()
          set add mm.head.toInt
          cmap.put(mm.head.toInt, set)
        } else {
          val cp = mm.head.toInt
          val higher = mm.drop(2).head.toInt
          val set = bmap.getOrElseUpdate(higher, new mutable.HashSet[Int]())
          set.add(cp)
        }
      }
    }
    for (cRoot <- cmap.keys) {
      val cSet = cmap.get(cRoot).get
      val tmpSet = bmap.getOrElse(cRoot, new mutable.HashSet[Int]())
      while (tmpSet.nonEmpty) {
        val head = tmpSet.head
        for (i <- bmap.getOrElse(head, new mutable.HashSet[Int]())) {
          tmpSet.add(i)
        }
        cSet.add(head)
        tmpSet.remove(head)
      }
    }

    val lb = new mutable.ListBuffer[String]
    for (cRoot <- cmap.keys) {
      val cList = cmap.get(cRoot).get.toList
      lb.append(cList.map(map.get(_).get).mkString("|"))
    }
    writeList(lb.toList, s"${baseDir}${threshold}clusters")
  }

  def main(args: Array[String]) {
    val system1 = ActorSystem("computerho")
    system1.actorOf(Props[ComputeRhoEfficient])
    system1.awaitTermination()
    val system2 = ActorSystem("computedelta")
    system2.actorOf(Props[ComputeDeltaEfficient])
    system2.awaitTermination()
    cluster()
  }
}

case object GimeWork

case class ID2XY(id: Int, x: Double, y: Double)

case class ID2xyRho(id: Int, x: Double, y: Double, rho: Int)

class ComputeRhoEfficient extends Actor {
  def checkContinuity(list: List[ID2XY]): Boolean = {
    var last = list.head.id
    for (curr <- list.tail) {
      if (curr.id - last != 1) return false
      last = curr.id
    }
    true
  }

  println(Calendar.getInstance.getTime)

  var allPoints = List.empty[ID2XY]
  for (file <- managed(io.Source.fromFile(s"${fastCluster.baseDir}gps2xy"))) {
    for (line <- file.getLines()) {
      val mm = line.split("\t")
      val xy = mm.last.split(",")
      allPoints = ID2XY(mm.head.toInt, xy.head.toDouble, xy.last.toDouble) :: allPoints
    }
  }

  allPoints = allPoints.sortBy(_.id)
  val points = allPoints

  if (checkContinuity(allPoints)) println("continues")
  else println("not continues")

  val array = Array.fill[Int](allPoints.last.id - allPoints.head.id + 1)(0)

  val router = context.actorOf(RoundRobinPool(fastCluster.MN).props(Props(new Actor {


    def receive = {
      case (point: ID2XY) :: (list: List[ID2XY]) =>
        for (p <- list) {
          if (math.sqrt(math.pow(p.x - point.x, 2) + math.pow(p.y - point.y, 2)) < fastCluster.threshold) {
            sender ! point.id
            sender ! p.id
          }
        }
        sender ! GimeWork
      case GimeWork =>
        sender ! GimeWork
    }
  })), "router")
  context watch router
  router ! Broadcast(GimeWork)

  def receive = {
    case id: Int =>
      array(id) += 1
    case GimeWork if allPoints.nonEmpty =>
      sender ! allPoints
      allPoints = allPoints.tail
      if (allPoints.isEmpty) router ! Broadcast(PoisonPill)
    case Terminated(ref) =>
      if (ref == router) {
        fastCluster.writeList(array.toList.zip(points).map { x => s"${x._2.id}\t${x._2.x},${x._2.y}\t${x._1}"}, s"${fastCluster.baseDir}${fastCluster.threshold}rhoe")
        context.system.shutdown()
      }
  }
}

class ComputeDeltaEfficient extends Actor {
  println(Calendar.getInstance.getTime)

  var allPoints = List.empty[ID2xyRho]
  for (file <- managed(io.Source.fromFile(s"${fastCluster.baseDir}${fastCluster.threshold}rhoe"))) {
    for (line <- file.getLines()) {
      val mm = line.split("\t")
      val xy = mm.tail.head.split(",")
      allPoints = ID2xyRho(mm.head.toInt, xy.head.toDouble, xy.last.toDouble, mm.last.toInt) :: allPoints
    }
  }
  allPoints = allPoints.sortBy(_.rho)

  val pw = context.actorOf(props = writeActor.props(s"${fastCluster.baseDir}${fastCluster.threshold}deltae"))
  context watch pw
  val router = context.actorOf(RoundRobinPool(fastCluster.MN).props(Props(new Actor {
    def receive = {
      case (point: ID2xyRho) :: (list: List[ID2xyRho]) =>
        var delta: Double = fastCluster.distance
        var np = -1
        for (p <- list if p.rho > point.rho) {
          val dist = math.sqrt(math.pow(p.x - point.x, 2) + math.pow(p.y - point.y, 2))
          if (dist < delta) {
            delta = dist
            np = p.id
          }
        }
        sender ! (point.id + "\t" + point.rho + "\t" + np + "\t" + delta)
        sender ! GimeWork
      case GimeWork =>
        sender ! GimeWork
    }
  })), "router")
  context watch router
  router ! Broadcast(GimeWork)

  def receive = {
    case result: String if result.size > 0 =>
      pw ! writeActor.Message(result)
    case GimeWork if allPoints.nonEmpty =>
      sender ! allPoints
      allPoints = allPoints.tail
      if (allPoints.isEmpty) router ! Broadcast(PoisonPill)
    case Terminated(ref) =>
      if (ref == router)
        pw ! PoisonPill
      if (ref == pw) {
        println(Calendar.getInstance.getTime)
        context.system.shutdown()
      }
  }
}

object writeActor {

  case class Message(msg: String)

  def props(desFile: String): Props = Props(new writeActor(desFile))
}

class writeActor(desFile: String) extends Actor {
  val pw = new PrintWriter(new FileWriter(desFile, false))

  def receive = {
    case writeActor.Message(msg) =>
      pw.println(msg)
  }

  override def postStop(): Unit = {
    pw.close()
    super.postStop()
  }
}