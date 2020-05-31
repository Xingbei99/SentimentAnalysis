package spark


import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

// The driver for the project
object SentimentAnalyzer {
    import Utils._

    // Configures Spark.
    // If we are not given master in configuration, we make a local deployment with one
    // executor running on one thread
    val sparkConf = new SparkConf()
                               .setAppName("streaming-data-analysis")
                               .setMaster(sys.env.get("spark.master").getOrElse("local[*]"))

    // Create SparkContext based on Spark properties set in sparkConf, which is used to create StreamingContext
    // SparkContext is connected to Spark cluster and therefore the main entry point to Spark functionalities
    val sparkContext = new SparkContext(sparkConf)

    // Create StreamingContext from an existing SparkContext, setting a window size of 5 seconds
    // The StreamingContext is used to create an input stream from Twitter
    // window size: every time we only process DStream in a window of 5 seconds
    // StreamingContext is the main entry point to spark streaming functions
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    // Creating a data stream from Twitter with non-English feeds filtered out.
    // DStream is a discretized stream, which consists of a continuous sequence of RDDs representing a
    // continuous stream of data. It is generated from live Twitter data here.
    //
    // Status is a status object provided by twitter4j. It is a data interface that contains many aspects of
    // information about a feed
    //
    // filter() is a transformation API on DStream
    // Return a new DStream by selecting only the records of the source DStream on which func returns true.
    //_.getLang: Use _ to refer to each Status object in input stream
    val feeds: DStream[Status] = TwitterUtils.createStream(streamingContext, None).filter(_.getLang == "en")

    // broadcast is an efficient way to distribute a copy of large dataset to each machine. It's read-only
    // and broadcast across the cluster. Each machine can access it locally
    val stopWords = sparkContext.broadcast(load("/stop-words.dat"))
    val posWords = sparkContext.broadcast(load("/pos-words.dat"))
    val negWords = sparkContext.broadcast(load("/neg-words.dat"))

    // Extract all words from tweets, stored in Sentence list in each result object in result DStream
    // map function applies the same function on every object in input stream and collects the result
    // objects, returning a new DStream
    // special function that can be applied with map: =>: transform schema of result objects
    val feedsText: DStream[(String, Seq[String])]
        = feeds.map(_.getText)
               .map(feedText => (feedText, getWordList(feedText))) // feedText represents the result string
                                                               // returned by getText operating on each
                                                               //status object

    // mapValues returns a new DStream by applying a map function to the value of
    // each key-value pairs in this DStream without changing the key.
    val meaningfulFeedsText: DStream[(String, Seq[String])]
       = feedsText.mapValues(toLower) // convert words to lowercase to ease processing
                  .mapValues(filterNonWords) // filter out non-words (with chars other than a-z)
                  // process each word list and filter out neutral words
                  // the value attribute of the broadcast variable stopWords get the broadcasted content
                  // Use => when there are multiple parameters, and current element is passed as one
                  // parameter
                  .mapValues(words => filterNeutralWords(words, stopWords.value))
                  // edge case: filter out empty word lists
                  // filter { case (_, valName) => retain expression }: filter key-value pairs by values
                  .filter { case (_, words) => words.nonEmpty }

    // Filtered neutral tweets by calculating the score of each tweet.
    val nonNeutralTextAndScore: DStream[(String, Int)]
       = meaningfulFeedsText.mapValues(words => getTweetScore(words, posWords.value, negWords.value))
                            .filter { case (_, score) => score != 0 }

    // Make key-value pairs the tweet text followed by its sentiment score, and print it
    nonNeutralTextAndScore.map(makeReadable).print
    // Save the result DStream objects into a json file
    nonNeutralTextAndScore.saveAsTextFiles("feeds", "json")
}
