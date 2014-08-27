package generic

import java.util.Calendar

import akka.actor._
import akka.routing.{Broadcast, RoundRobinPool}

import scala.collection.mutable
import scala.util.control.Breaks._

/**
 * Created by jwhu on 14-8-25.
 * At 下午3:49
 */
class FastCluster[T <: ComputableItem[T]]() {
  def computeRhoDelta(list: List[T], threshold: Double, flag: Boolean = false, MN: Int = Runtime.getRuntime.availableProcessors()): List[(String, Int, Double)] = {
    def getDistance(): Double = {
      val nSamples = list.size
      val nLow = 0.016 * nSamples * nSamples
      val nHigh = 0.020 * nSamples * nSamples

      var dc = 0.0
      var neighbors = 0
      println(s"nLow = $nLow, nHigh = $nHigh")

      while (neighbors < nLow || neighbors > nHigh) {
        neighbors = 0
        breakable {
          for {
            sample <- list
            sample2 <- list
            if (sample != sample2)
          } {
            if (sample.computeDistance(sample2) <= dc) neighbors += 1
            if (neighbors > nHigh) break()
          }
        }
        dc += 0.03
        println(s"dc = $dc, neighbors = $neighbors")
      }
      dc
    }
    val th = if (flag) getDistance() else threshold
    val resList = new mutable.ListBuffer[(String, Int, Double)]

    class FooActor extends Actor {

      case object GiMeWork

      case class Id2Item(id: Int, item: T)

      case class Id2ItemRho(id: Int, item: T, rho: Int)

      case class Res(item: T, rho: Int, delta: Double)

      case class RhoJob(point: Id2Item, list: List[Id2Item])

      case class DeltaJob(point: Id2ItemRho, list: List[Id2ItemRho])

      println(Calendar.getInstance.getTime)

      val allPoints = list.zipWithIndex.map(x => Id2Item(x._2, x._1))

      val array = Array.fill[Int](allPoints.last.id + 1)(0)

      var router = context.actorOf(RoundRobinPool(MN).props(Props(new Actor {

        def receive = {
          case RhoJob(point, list) =>
            val item = point.item
            for (p <- list) {
              if (item.computeDistance(p.item) < th) {
                sender ! point.id
                sender ! p.id
              }
            }
            sender ! GiMeWork
          case DeltaJob(point, list) =>
            var delta = Double.MaxValue
            val item = point.item
            var flag = true

            for {
              p <- list
              if p.rho > point.rho
            } {
              val dist = item.computeDistance(p.item)
              if (dist < delta) {
                delta = dist
                flag = false
              }
            }
            if (flag) {
              delta = Double.MinValue
              for (p <- allPoints) {
                val dist = item.computeDistance(p.item)
                if (dist > delta) delta = dist
              }
            }
            sender ! Res(point.item, point.rho, delta)
            sender ! GiMeWork
          case GiMeWork =>
            sender ! GiMeWork
        }
      })), "router")
      context watch router
      self ! GiMeWork

      def receive = {
        case GiMeWork =>
          context become (ComputeRho(allPoints, MN), discardOld = true)
          router ! Broadcast(GiMeWork)
      }

      def ComputeRho(points: List[Id2Item], jn: Int): Receive = {
        case id: Int =>
          array(id) += 1
        case GiMeWork =>
          if (points.nonEmpty) {
            sender ! RhoJob(points.head, points.tail)
            context become (ComputeRho(points.tail, jn), discardOld = true)
          } else if (jn > 1) {
            context become (ComputeRho(points, jn - 1), discardOld = true)
          } else {
            context.become(ComputeDelta(array.toList.zip(allPoints).map(x => Id2ItemRho(x._2.id, x._2.item, x._1)).sortBy(_.rho), MN), discardOld = true)
            router ! Broadcast(GiMeWork)
          }
      }

      def ComputeDelta(points: List[Id2ItemRho], jn:Int): Receive = {
        case Res(item, rho, delta) =>
          resList.append((item.toString, rho, delta))
        case GiMeWork =>
          if(points.nonEmpty) {
            sender ! DeltaJob(points.head, points.tail)
            context become(ComputeDelta(points.tail, jn), discardOld = true)
          } else if(jn > 1){
            context become(ComputeDelta(points, jn - 1), discardOld = true)
          }else{
            router ! Broadcast(PoisonPill)
          }
        case Terminated(ref) =>
          if (ref == router) {
            println(Calendar.getInstance.getTime)
            context.system.shutdown()
          }
      }
    }

    val system = ActorSystem()
    system.actorOf(Props(new FooActor))
    system.awaitTermination()
    resList.toList
  }

  def cluster(list: List[(T, Int, Double)], threshold: Double, validate: (Int, Double) => Boolean): List[String] = {
    println(Calendar.getInstance.getTime)
    var resList = List.empty[(T, Int)]
    var cn = 0

    def compute(item: T): (Double, Int) = {
      var dist = Double.MaxValue
      var idx = -1
      for ((i, c) <- resList) {
        val d = item.computeDistance(i)
        if (d < dist) {
          dist = d
          idx = c
        }
      }
      (dist, idx)
    }

    for (ele <- list if validate(ele._2, ele._3)) {
      val (d, idx) = compute(ele._1)
      if (d < threshold) resList = (ele._1, idx) :: resList
      else {
        cn += 1
        resList = (ele._1, cn) :: resList
      }
    }

    var data = list.sortBy(-_._2)

    while (data.nonEmpty) {
      val rho = data.head._2
      val set = new mutable.HashSet[T]
      while (data.nonEmpty && data.head._2 == rho) {
        set.add(data.head._1)
        data = data.tail
      }

      while (set.nonEmpty) {
        var item = set.head
        var (dist, idx) = compute(item)
        for (it <- set) {
          val (d, i) = compute(it)
          if (d < dist) {
            dist = d
            idx = i
            item = it
          }
        }
        resList = (item, idx) :: resList
        set.remove(item)
      }

    }
    println(Calendar.getInstance.getTime)
    resList.map(x => s"${x._1.toString},${x._2}")
  }
}

//object writeActor {
//
//  case class Message(msg: String)
//
//  def props(desFile: String): Props = Props(new writeActor(desFile))
//}
//
//class writeActor(desFile: String) extends Actor {
//  val pw = new PrintWriter(new FileWriter(desFile, false))
//
//  def receive = {
//    case writeActor.Message(msg) =>
//      pw.println(msg)
//  }
//
//  override def postStop(): Unit = {
//    pw.close()
//    super.postStop()
//  }
//}