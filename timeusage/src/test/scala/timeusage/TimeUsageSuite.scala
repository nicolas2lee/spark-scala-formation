package timeusage

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {


  test("'classifiedColumns' should return 3 list") {
    val columnNames = List("t010101", "t030101", "t110101", "t180101", "t180301",
      "t050101", "t180501",
      "t020101", "t040101")
    val result = classifiedColumns(columnNames)
    println(result._1)
    assert(result._1.size == 5)
    println(result._2)
    assert(result._2.size == 2)
    println(result._3)
    assert(result._3.size == 2)
  }
  /*
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    */
  test("'timeUsageSummary' should return correct result") {
    val (columns, initDf) = read("/timeusage/atussum_test.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    initDf.sort()
    //initDf.select(when($"telfs">= 1 and ($"telfs"<3),"working").otherwise("not working")).show(5)
    //val result = initDf.select(when($"telfs"))
    //result.show(5)
    //val result = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

  }

}
