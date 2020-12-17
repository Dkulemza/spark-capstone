package tweets.user

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, count, col, asc}
import org.apache.spark.sql.{Dataset, Row}

object TweeterStatistic {

  val spark: SparkSession =
    SparkSession.builder().master("local").getOrCreate()

  def main(args: Array[String]): Unit = {
    val firstWaveTopTen = calculateFirstWaveTopTen(
      "src/main/resources/twitter-data/retweet.avro",
      "src/main/resources/twitter-data/message_user.avro",
      "src/main/resources/twitter-data/user_dir.avro",
      "src/main/resources/twitter-data/message_dir.avro"
    )

    firstWaveTopTen.show

    val secondWaveTopTen = calculateSecondWaveTopTen(
      "src/main/resources/twitter-data/retweet.avro",
      "src/main/resources/twitter-data/message_user.avro",
      "src/main/resources/twitter-data/user_dir.avro",
      "src/main/resources/twitter-data/message_dir.avro"
    )

    secondWaveTopTen.show

  }

  def calculateFirstWaveTopTen(retweetPath: String,
                               messageUserPath: String,
                               userPath: String,
                               messagePath: String): Dataset[Row] = {

    val userDf = TweeterReader.readUserData(userPath)
    val messageDf = TweeterReader.readMessageData(messagePath)
    val messageUserDf = TweeterReader.readMessageUserData(messageUserPath)
    val retweetDf = TweeterReader.readRetweetData(retweetPath)

    val userMessage = userDf
      .join(messageUserDf.join(messageDf, Seq("MESSAGE_ID")), Seq("USER_ID"))

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

  def calculateSecondWaveTopTen(retweetPath: String,
                                messageUserPath: String,
                                userPath: String,
                                messagePath: String): Dataset[Row] = {

    val userDf = TweeterReader.readUserData(userPath)
    val messageDf = TweeterReader.readMessageData(messagePath)
    val messageUserDf = TweeterReader.readMessageUserData(messageUserPath)
    val retweetDf = TweeterReader.readRetweetData(retweetPath)

    val userMessage = userDf
      .join(messageUserDf.join(messageDf, Seq("MESSAGE_ID")), Seq("USER_ID"))

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
