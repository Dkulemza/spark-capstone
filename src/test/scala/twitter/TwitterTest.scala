package twitter

import org.scalatest._
import tweets.user.TweeterStatistic

class TwitterTest extends FunSuite {
  test("TweeterStatistic.getFirstWaveTopTen") {
    val actual = TweeterStatistic.getFirstWaveTopTen.rdd.map(r => (r(0), r(1), r(2), r(3), r(4), r(5))).collect.toList
    val expectedValue = (4, "Olivia", "Smith", 1, "message", 1)
    assert(
      actual(7) === expectedValue
    )
  }

  test("TweeterStatistic.getSecondWaveTopTen") {
    val actual = TweeterStatistic.getSecondWaveTopTen.rdd.map(r => (r(0), r(1), r(2), r(3), r(4), r(5))).collect.toList
    val expectedValue = (4, "Olivia", "Smith", 1, "message", 2)
    assert(
      actual(1) === expectedValue
    )
  }


}
