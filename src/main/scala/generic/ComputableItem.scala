package generic

/**
 * Created by jwhu on 14-8-25.
 * At 下午3:17
 */
trait ComputableItem[T <: ComputableItem[T]] {
  //} extends Ordered[ComparableItem[T]]{

  @inline
  def computeDistance(that: T): Double

  override abstract def toString: String = getString()

  @inline
  def getString(): String
}
