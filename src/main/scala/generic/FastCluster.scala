package generic

import java.util.Calendar
import akka.actor._
import akka.routing.{Broadcast, RoundRobinPool}

import scala.annotation.tailrec
import scala.collection.immutable.{HashSet, HashMap}
import scala.collection.mutable
import scala.util.control.Breaks._

/**
* Created by jwhu on 14-8-25.
* At 下午3:49
*/
class FastCluster[T <: ComputableItem[T]](val MN: Int = Runtime.getRuntime.availableProcessors()) {
  def computeRhoDelta(list: List[T], threshold: Double, flag: Boolean = false): List[(String, Int, Double)] = {
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
    def compute(item: T, list: List[(T, Int)]): (Double, Int) = {
      var dist = Double.MaxValue
      var idx = -1
      for ((i, c) <- list) {
        val d = item.computeDistance(i)
        if (d < dist) {
          dist = d
          idx = c
        }
      }
      (dist, idx)
    }
    var cn = 0
    var res = List.empty[(T, Int)]
    class FooActor extends Actor {

      case object GiMeWork

      case class InitJob(ele: T, rho: Int, delta: Double)

      case class InitRootResult(ele: T, rho: Int, isRoot: Boolean)

      case class ComputeJob(ele: T, list: List[(T, Int)])

      case class ComputeResult(e: T, d: Double, i: Int)

      println(Calendar.getInstance.getTime)

      val router = context.actorOf(RoundRobinPool(MN).props(Props(new Actor {
        def receive = {
          case InitJob(ele, rho, delta) =>
            if (validate(rho, delta)) sender ! InitRootResult(ele, rho, true)
            else sender ! InitRootResult(ele, rho, false)
            sender ! GiMeWork
          case ComputeJob(ele, list) =>
            val (d, idx) = compute(ele, list)
            sender ! ComputeResult(ele, d, idx)
            sender ! GiMeWork
          case GiMeWork =>
            sender ! GiMeWork
        }
      })), "router")
      context watch router

      self ! GiMeWork

      def receive = {
        case GiMeWork =>
          router ! Broadcast(GiMeWork)
          context become InitState(List.empty[(T, Int)], list, MN)
      }

      def InitState(data: List[(T, Int)], list: List[(T, Int, Double)], jn: Int): Receive = {
        case InitRootResult(ele, rho, isRoot) =>
          if (isRoot) {
            val (d, idx) = compute(ele, res)
            if (d < threshold) res = (ele, idx) :: res
            else {
              cn += 1
              res = (ele, cn) :: res
            }
            context become InitState(data, list, jn)
          } else context become InitState((ele, rho) :: data, list, jn)
        case GiMeWork =>
          if (list.nonEmpty) {
            val head = list.head
            router ! InitJob(head._1, head._2, head._3)
            context become InitState(data, list.tail, jn)
          } else if (jn > 1) {
            context become InitState(data, list, jn - 1)
          } else {
            val lbRhoSet = new mutable.ListBuffer[Set[T]]
            val sortedData = data.sortBy(-_._2)
            var rho = sortedData.head._2
            val set = new mutable.HashSet[T]()
            for (ele <- sortedData) {
              if (ele._2 > rho) {
                rho = ele._2
                lbRhoSet.append(set.toSet)
                set.clear()
              }
              set add ele._1
            }
            lbRhoSet.append(set.toSet)
            val RhoSetList = lbRhoSet.toList
            context become ComputeState(RhoSetList.head, RhoSetList.head, RhoSetList.tail, Double.MaxValue, -1, null.asInstanceOf[T], MN)
            router ! Broadcast(GiMeWork)
          }
      }

      def ComputeState(remainSet: Set[T], rhoSet: Set[T], rhoSetList: List[Set[T]], minDist: Double, idx: Int, ele: T, jn: Int): Receive = {
        case ComputeResult(e: T, d: Double, i: Int) =>
          if (d < minDist) context become (ComputeState(remainSet, rhoSet, rhoSetList, d, i, e, jn), discardOld = true)
        case GiMeWork =>
          if (remainSet.nonEmpty) {
            val head = remainSet.head
            sender ! ComputeJob(head, res)
            context become (ComputeState(remainSet - head, rhoSet, rhoSetList, minDist, idx, ele, jn), discardOld = true)
          } else if (jn > 1) {
            context become (ComputeState(remainSet, rhoSet, rhoSetList, minDist, idx, ele, jn - 1), discardOld = true)
          } else if (rhoSet.nonEmpty) {
            val set = rhoSet - ele
            res = (ele, idx) :: res
            context become (ComputeState(set, set, rhoSetList, Double.MaxValue, -1, null.asInstanceOf[T], MN), discardOld = true)
            router ! Broadcast(GiMeWork)
          } else if (rhoSetList.nonEmpty) {
            context become (ComputeState(rhoSetList.head, rhoSetList.head, rhoSetList.tail, Double.MaxValue, -1, null.asInstanceOf[T], MN), discardOld = true)
            router ! Broadcast(GiMeWork)
          } else {
            context become (finishState, discardOld = true)
            router ! Broadcast(PoisonPill)
          }
      }

      def finishState:Receive={
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
    res.map(x => s"${x._1.toString},${x._2}")
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