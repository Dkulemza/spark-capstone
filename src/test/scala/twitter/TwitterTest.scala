package twitter

import org.scalatest._
import tweets.user.TweeterStatistic

class TwitterTest extends FunSuite {
  test("TweeterStatistic.getFirstWaveTopTen") {
    val actual = TweeterStatistic
      .calculateFirstWaveTopTen(
        "src/test/resources/retweet.avro",
        "src/test/resources/message_user.avro",
        "src/test/resources/user_dir.avro",
        "src/test/resources/message_dir.avro"
      )
      .rdd
      .map(r => (r(0), r(1), r(2), r(3), r(4), r(5)))
      .collect
      .toList
    val expectedValue = (12, "Jacob", "Smith", 2, "tweet", 2)
    assert(actual(7) === expectedValue)
  }

  test("TweeterStatistic.getSecondWaveTopTen") {
    val actual = TweeterStatistic
      .calculateSecondWaveTopTen(
        "src/test/resources/retweet.avro",
        "src/test/resources/message_user.avro",
        "src/test/resources/user_dir.avro",
        "src/test/resources/message_dir.avro"
      )
      .rdd
      .map(r => (r(0), r(1), r(2), r(3), r(4), r(5)))
      .collect
      .toList
    val expectedValue = (8, "Jayden", "Johnson", 3, "something", 2)
    assert(actual(1) === expectedValue)
  }

}
