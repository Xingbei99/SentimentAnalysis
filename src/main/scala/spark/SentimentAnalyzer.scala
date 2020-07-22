package spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

/*
    Performs real time sentiment analysis of twitter feeds.
    This object is the driver for the project.
 */
object SentimentAnalyzer extends App{
    import Utils._

    // Configures Spark.
    val sparkConf: SparkConf = new SparkConf()
                                         .setAppName("streaming-data-analysis")
                                         .setMaster(sys.env.get("spark.master").getOrElse("local[*]"))

    // Defines SparkContext, the main entry point to Spark functionalities
    val sparkContext = new SparkContext(sparkConf)

    // Defines StreamingContext, the main entry point to spark streaming functions
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    // Creates an input stream of tweets from Twitter with non-English feeds filtered out.
    // We will mainly use DStream APIs to transform and analyze the data
    val tweetObjStream: DStream[Status] = TwitterUtils.createStream(streamingContext, None).filter(_.getLang == "en")


    // Loads word libraries with broadcast API of Spark.
    val stopWords: Broadcast[Set[String]] = sparkContext.broadcast(load("/stop-words.dat"))
    val posWords: Broadcast[Set[String]] = sparkContext.broadcast(load("/pos-words.dat"))
    val negWords: Broadcast[Set[String]] = sparkContext.broadcast(load("/neg-words.dat"))

    // Transforms the DStream of tweets.
    // (1) Transform each tweet object of the DStream to a key-value pair of tweet text : list of
    //     words of the tweet text to ease printing and analyzing
    val textAndWordsStream: DStream[(String, Seq[String])]
                                = tweetObjStream.map(_.getText)
                                                .map(feedText => (feedText, getWordList(feedText)))

    // (2) Converts the word list of each tweet to lowercase to ease filtering and analyzing
    // (3) Filters out non-words and neutral words
    // (4) Filters out tweets with no meaningful words
    val meaningfulFeedsText: DStream[(String, Seq[String])]
                = textAndWordsStream.mapValues(toLower)
                                    .mapValues(filterNonWords)
                                    .mapValues(words => filterNeutralWords(words, stopWords.value))
                                    .filter { case (_, words) => words.nonEmpty }

    // (5) Transforms each key-value pair of the stream to a key-value pair of text of the tweet : its
    //     sentiment score
    val nonNeutralTextAndScore: DStream[(String, Int)]
       = meaningfulFeedsText.mapValues(words => getTweetScore(words, posWords.value, negWords.value))
                            .filter { case (_, score) => score != 0 }

    // Make key-value pairs the tweet text followed by its sentiment score, and print it
    nonNeutralTextAndScore.map(makeReadable).print
    // Save the result DStream objects into a json file
    nonNeutralTextAndScore.saveAsTextFiles("feeds", "json")

    // Starts streaming of the created DStream.
    streamingContext.start()

    // Waits for context.stop() or the throwing of an exception.
    streamingContext.awaitTermination()
}
