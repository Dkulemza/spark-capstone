package tweets.user

import tweets.user.TweeterStatistic.spark

object TweeterReader {

  def readUserData(path: String) = {
    spark.read
      .format("avro")
      .load(path)
      .toDF()

  }

  def readMessageData(path: String) = {
    spark.read
      .format("avro")
      .load(path)
      .toDF()
  }
  def readMessageUserData(path: String) = {
    spark.read
      .format("avro")
      .load(path)
      .toDF()
  }
  def readRetweetData(path: String) = {
    spark.read
      .format("avro")
      .load(path)
      .toDF()
  }

}
