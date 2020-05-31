package spark

import scala.io.{AnsiColor, Source}

// Defines helper functions for SentimentAnalyzer
object Utils {
  
  def load(resourcePath: String): Set[String] = {
    // Source.fromInputStream reads data from input string into Source object
    // getClass.getResourceAsStream uses class loader to load the resource
    val source = Source.fromInputStream(getClass.getResourceAsStream(resourcePath))
    // Store words from source object into a set
    val words = source.getLines.toSet
    source.close()
    words
  }

  // Extract all words of the passed tweet text and store them in a list
  def getWordList(tweetText: String): Seq[String] = {
      tweetText.split(" ")
  }
  // Convert each word in wordList to lower case
  def toLower(wordList: Seq[String]): Seq[String] = {
    // Seq type has map function that processes each element of the sequence
    wordList.map(_.toLowerCase)
  }

  // Only keep words with only letters in a given word list
  def filterNonWords(wordList: Seq[String]): Seq[String] = {
    // Seq type has filter function that preserves the elements satisfying the passed condition
    // matches() returns true if the string matches the regular expression
    // + in regex: the string matches the previous expression of + more than once
    wordList.filter(_.matches("[a-z]+"))
  }

  // Filters out all neutral words in a given word list and returns a new list
  def filterNeutralWords(wordList: Seq[String], neutralWords: Set[String]): Seq[String] = {
    // filterNot returns a new list consisting all the elements of the list
    // which does not satisfies the given predicate.
    wordList.filterNot(word => neutralWords.contains(word))
  }

  // Calculate the score of a given tweet by adding up scores of its words
  def getTweetScore(wordList: Seq[String], posWords: Set[String], negWords: Set[String]): Int = {
    wordList.map(word => getWordScore(word, posWords, negWords)).sum
  }

  // Get the score of a given word
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

