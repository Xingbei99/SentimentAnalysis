package spark

import scala.io.{AnsiColor, Source}

/**
 *  Defines helper functions for SentimentAnalyzer
 */
object Utils {
  /**
   * Read the file from resources folder and convert it to a set.
   * @param resourcePath: path of the file to read
   * @return The file converted to a set
   */
  def load(resourcePath: String): Set[String] = {
    val bufSource = Source.fromInputStream(getClass.getResourceAsStream(resourcePath))
    val words = bufSource.getLines.toSet
    bufSource.close() // Close the source while not using it
    words
  }

  /**
   * Extract all words of the passed tweet text and store them in a list
   * @param tweetText: tweet text
   * @return
   */
  def getWordList(tweetText: String): Seq[String] = {
      tweetText.split(" ")
  }

  /**
   * Convert each word in wordList to lower case
   * @param wordList: list of all words in the text of current tweet
   * @return
   */
  def toLower(wordList: Seq[String]): Seq[String] = {
    wordList.map(_.toLowerCase)
  }

  /**
   * Only keep words with only letters in a given word list
   * @param wordList: list of all words in the text of current tweet
   * @return
   */
  def filterNonWords(wordList: Seq[String]): Seq[String] = {
    wordList.filter(_.matches("[a-z]+"))
  }

  /**
   * Filters out all neutral words in a given word list and returns a new list
   * @param wordList: list of all words in the text of current tweet after preprocessing
   * @param neutralWords: neutral words library
   * @return
   */
  def filterNeutralWords(wordList: Seq[String], neutralWords: Set[String]): Seq[String] = {
    wordList.filterNot(word => neutralWords.contains(word))
  }

  /**
   *
   * @param wordList: list of all words in the text of current tweet after preprocessing
   * @param posWords: positive words library
   * @param negWords: negative words library
   * @return The sentiment score of the current tweet
   */
  def getTweetScore(wordList: Seq[String], posWords: Set[String], negWords: Set[String]): Int = {
    wordList.map(word => getWordScore(word, posWords, negWords))
            .sum
  }

  /**
   * Get the score of a given word
   * @param word: input word
   * @param posWords
   * @param negWords
   * @return The sentiment score of the given word
   */
  def getWordScore(word: String, posWords: Set[String], negWords: Set[String]): Int = {
    if (posWords.contains(word)) 1
    else if (negWords.contains(word)) -1
    else 0
  }

  private def format(n: Int): String = {
    f"$n%2d"
  }

  private def formatScore(s: String): String = {
    s"[ $s ] "
  }

  private def makeScoreReadable(n: Int): String = {
    if (n > 0)      s"${AnsiColor.GREEN + format(n) + AnsiColor.RESET}"
    else if (n < 0) s"${AnsiColor.RED   + format(n) + AnsiColor.RESET}"
    else            s"${format(n)}"
  }

  private def makeTweetReadable(s: String): String = {
    s.takeWhile(_ != '\n').take(80) + "..." //short it ?
  }

  def makeReadable(sn: (String, Int)): String = {
    sn match {
      case (tweetText, score) => s"${formatScore(makeScoreReadable(score))}${makeTweetReadable(tweetText)}"
    }
  }

}

