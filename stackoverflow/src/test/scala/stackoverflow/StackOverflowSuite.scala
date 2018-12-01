package stackoverflow


@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  override def afterAll(): Unit = {
    StackOverflow.sc.stop()
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("groupedPostings simple test") {
    val postings = List(
      //postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]
      Posting(1, 1, None, None, 1, None),
      Posting(2, 2, None, Some(1), 2, None),
      Posting(2, 3, None, Some(1), 3, None)
    )
    val rdd = StackOverflow.sc.parallelize(postings)
    val result = StackOverflow.groupedPostings(rdd).collect().toList
    assert(result.head._1 == 1)
  }

  test("scoredPostings simple test") {
    val postings = List(
      //postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]
      Posting(1, 1, None, None, 1, None),
      Posting(2, 2, None, Some(1), 2, None),
      Posting(2, 3, None, Some(1), 3, None)
    )
    val rdd = StackOverflow.sc.parallelize(postings)
    val grouped = StackOverflow.groupedPostings(rdd)
    val result = StackOverflow.scoredPostings(grouped).collect().toList

    assert(result.head._2 == 3)
  }

}
