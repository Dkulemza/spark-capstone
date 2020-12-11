package tweets.user

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, count, col, asc}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object TweeterStatistic {

  private val spark: SparkSession =
    SparkSession.builder().master("local").getOrCreate()

  private val userDf = spark.read
    .format("avro")
    .load("src/main/twitter-data/user_dir.avro")
    .toDF()

  private val messageDf = spark.read
    .format("avro")
    .load("src/main/twitter-data/message_dir.avro")
    .toDF()

  private val messageUserDf = spark.read
    .format("avro")
    .load("src/main/twitter-data/message_user.avro")
    .toDF()

  private val retweetDf = spark.read
    .format("avro")
    .load("src/main/twitter-data/retweet.avro")
    .toDF()

  private val userMessage = userDf
    .join(messageUserDf.join(messageDf, Seq("MESSAGE_ID")), Seq("USER_ID"))

  def main(args: Array[String]): Unit = {
    val firstWaveTopTen = getFirstWaveTopTen
    firstWaveTopTen.show

    val secondWaveTopTen = getSecondWaveTopTen

    secondWaveTopTen.show

  }

  def getFirstWaveTopTen: Dataset[Row] = {
    val firstTopTen =
      retweetDf.groupBy("USER_ID").agg(count("*").as("NUMBER_RETWEETS"))

    firstTopTen
      .join(userMessage, Seq("USER_ID"))
      .select(
        "USER_ID",
        "FIRST_NAME",
        "LAST_NAME",
        "MESSAGE_ID",
        "TEXT",
        "NUMBER_RETWEETS"
      )
      .orderBy(desc("NUMBER_RETWEETS"), asc("USER_ID"))
      .limit(10)

  }

  def getSecondWaveTopTen: Dataset[Row] = {
    val secondWave = retweetDf
      .as("retweet1")
      .join(
        retweetDf.as("retweet2"),
        col("retweet1.SUBSCRIBER_ID") === col("retweet2.USER_ID") &&
          col("retweet1.MESSAGE_ID") === col("retweet2.MESSAGE_ID")
      )
      .groupBy("retweet1.SUBSCRIBER_ID")
      .agg(count("retweet1.SUBSCRIBER_ID").as("NUMBER_RETWEETS"))

    secondWave
      .as("secondWave")
      .join(
        userMessage.as("msg"),
        secondWave("SUBSCRIBER_ID") === userMessage("USER_ID")
      )
      .select(
        "msg.USER_ID",
        "msg.FIRST_NAME",
        "msg.LAST_NAME",
        "msg.MESSAGE_ID",
        "msg.TEXT",
        "secondWave.NUMBER_RETWEETS"
      )
      .orderBy(desc("secondWave.NUMBER_RETWEETS"))
      .limit(10)

  }

}
