package sparkcourse.distcp.utils

import java.net.URI

import org.apache.hadoop.fs.Path
import sparkcourse.sparkdistcp.objects.SparkDistCpOptions

object OptionsParsing {

  def parse(args: Array[String]): Config = {

    val parser = new scopt.OptionParser[Config]("") {
      opt[Unit]("i")
        .action((_, c) => c.copyOptions(_.copy(ignoreErrors = true)))
        .text("Ignore failures")

      opt[Int]("m")
        .action((i, c) => c.copyOptions(_.copy(maxConcurrency = i)))
        .text("max concurrency")

      opt[Unit]("verbose")
          .action((_, c) => c.copyOptions(_.copy(verbose = true)))
          .text("verbose mode")

      arg[String]("[source_path...] <target_path>")
        .unbounded()
        .minOccurs(2)
        .action((u, c) => c.copy(URIs = c.URIs :+ new URI(u)))

      help("help").text("help text")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        config.options.validateOptions()
        config
      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }

  }
}


case class Config(options: SparkDistCpOptions = SparkDistCpOptions(), URIs: Seq[URI] = Seq.empty) {

  def copyOptions(f: SparkDistCpOptions => SparkDistCpOptions): Config = {
    this.copy(options = f(options))
  }

  def sourceAndDestPaths: (String, String) = {
    val sourcePath = URIs.takeRight(URIs.length).head
    val targetPath = URIs.takeRight(URIs.length -1).head
    (sourcePath.toString, targetPath.toString)
  }

}