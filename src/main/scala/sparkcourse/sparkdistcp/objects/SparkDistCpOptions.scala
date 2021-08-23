package sparkcourse.sparkdistcp.objects


case class SparkDistCpOptions(ignoreErrors: Boolean = SparkDistCpOptions.Defaults.ignoreErrors,
                              maxConcurrency: Int = SparkDistCpOptions.Defaults.maxConcurrency,
                              verbose: Boolean = SparkDistCpOptions.Defaults.verbose
                             ) {

  def validateOptions(): Unit = {
    assert(maxConcurrency > 0, "concurrent must > 0")

  }

}

object SparkDistCpOptions {

  object Defaults {
    val ignoreErrors: Boolean = false
    val maxConcurrency: Int = 1
    val verbose: Boolean = false
  }

}
