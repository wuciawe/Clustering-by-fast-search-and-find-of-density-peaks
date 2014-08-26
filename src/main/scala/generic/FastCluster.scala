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
  def computeRhoDelta(list: List[T], threshold: Double, flag: Boolean, MN: Int = Runtime.getRuntime.availableProcessors()): List[(String, Int, Double)] = {
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
    val system = ActorSystem()
    system.actorOf(Props(new Actor {

      case object GiMeWork

      case class Id2Item(id: Int, item: T, rho: Int = 0)

      case class Res(id2item: Id2Item, delta: Double)

      println(Calendar.getInstance.getTime)

      var points = list.zipWithIndex.map(x => Id2Item(x._2, x._1))
      var allPoints = points

      val array = Array.fill[Int](allPoints.last.id + 1)(0)

      var router = context.actorOf(RoundRobinPool(MN).props(Props(new Actor {

        def receive = {
          case (point: Id2Item) :: (list: List[Id2Item]) =>
            val item = point.item
            for (p <- list) {
              if (item.computeDistance(p.item) < th) {
                sender ! point.id
                sender ! p.id
              }
            }
            sender ! GiMeWork
          case GiMeWork =>
            sender ! GiMeWork
        }
      })), "router")
      context watch router
      router ! Broadcast(GiMeWork)

      def receive = {
        case id: Int =>
          array(id) += 1
        case GiMeWork if allPoints.nonEmpty =>
          sender ! allPoints
          allPoints = allPoints.tail
          if (allPoints.isEmpty) router ! Broadcast(PoisonPill)
        case Terminated(ref) =>
          if (ref == router) {
            context.unwatch(router)
            context.become(behavior, discardOld = true)
            allPoints = array.toList.zip(points).map(x => Id2Item(x._2.id, x._2.item, x._1)).sortBy(_.rho)
            points = allPoints
            router = context.actorOf(RoundRobinPool(MN).props(Props(new Actor {
              def receive = {
                case list: List[Id2Item] =>
                  var delta = Double.MaxValue
                  val point = list.head
                  val item = point.item
                  var flag = true

                  for {
                    p <- list.tail
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
                    for (p <- points) {
                      val dist = item.computeDistance(p.item)
                      if (dist > delta) delta = dist
                    }
                  }
                  sender ! Res(point, delta)
                  sender ! GiMeWork
                case GiMeWork =>
                  sender ! GiMeWork
              }
            })), "router")
            context watch router
            router ! Broadcast(GiMeWork)
          }
      }

      def behavior: Receive = {
        case result: Res =>
          resList.append((result.id2item.item.toString, result.id2item.rho, result.delta))
        case GiMeWork if allPoints.nonEmpty =>
          sender ! allPoints
          allPoints = allPoints.tail
          if (allPoints.isEmpty) router ! Broadcast(PoisonPill)
        case Terminated(ref) =>
          if (ref == router) {
            println(Calendar.getInstance.getTime)
            context.system.shutdown()
          }
      }
    }))
    system.awaitTermination()
    resList.toList
  }

  def cluster(list: List[(T, Int, Double)], threshold: Double, validate: (Int, Double) => Boolean): List[List[String]] = {

    val resList = new mutable.OpenHashMap[Int, mutable.ListBuffer[T]]

    def compute(item: T): (Double, Int) = {
      var dist = Double.MaxValue
      var idx = -1
      for {
        (key, list) <- resList
        i <- list
      } {
        val d = item.computeDistance(i)
        if (d < dist) {
          dist = d
          idx = key
        }
      }
      (dist, idx)
    }

    for (ele <- list if validate(ele._2, ele._3)) {
      val (d, idx) = compute(ele._1)
      if (d < threshold) resList.get(idx).get.append(ele._1)
      else {
        val lb = new mutable.ListBuffer[T]
        lb.append(ele._1)
        resList.put(resList.size, lb)
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
        resList.get(idx).get.append(item)
        set.remove(item)
      }

    }
    resList.values.toList.map(_.toList.map(_.toString))
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