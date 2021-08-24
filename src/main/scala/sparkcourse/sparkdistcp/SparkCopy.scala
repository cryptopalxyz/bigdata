package sparkcourse.sparkdistcp

//使用FileSystem类FileStatus区分目录和文件，filter出目录，递归创建目录
//FileSystem类下吗的mkdirs


import groovy.lang.Tuple
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import sparkcourse.distcp.utils.OptionsParsing
import sparkcourse.sparkdistcp.interfaces.Logging
import sparkcourse.sparkdistcp.objects.{ConfigSerDeser}

import scala.collection.mutable.ArrayBuffer

object SparkCopy extends Logging with App {

  override def main(args: Array[String]): Unit = {
    println("hello world")
    val config = OptionsParsing.parse(args)
    //val sourceFs = new Path(sourceURI).getFileSystem(sparkContext.hadoopConfiguration)
    val (sourcePathString, targetPathString) = config.sourceAndDestPaths
    //run(spark, src, dest, config.options)
    val sourcePath = new Path(sourcePathString)
    val sourcePathRoot = sourcePath
    val targetPath = new Path(targetPathString)
    val spark = SparkSession.builder().appName("spark-distcp").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")
    setLogLevel(Level.DEBUG)
    val filesFailed: LongAccumulator = spark.sparkContext.longAccumulator("FilesFailed")
    var fileList = ArrayBuffer[(Path, Path)]()
    var folderList = ArrayBuffer[FileStatus]()

    val fs = sourcePath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    checkDirectories(sourcePath, sourcePathRoot, targetPath, spark.sparkContext, fileList, folderList)
    val fileStringList = fileList.map((s) => (s._1.toString(), s._2.toString))

    val rdd = spark.sparkContext.makeRDD(fileStringList, config.options.maxConcurrency)
    val sparkContext = spark.sparkContext
    /*
    //map是在excutor中进行计算等操作的，而SparkSession是属于Driver端的组件或者服务，怎么能放到excutor中去呢？
    //把config Ser之后发到executor，避免产生spark session
    rdd.map {
      file =>
        val f1 = new Path(file._1)
        val newSpark = SparkSession.builder().getOrCreate()
        val hadoopConf = newSpark.sparkContext.hadoopConfiguration
        val f2 = f1.getFileSystem(hadoopConf)
        val fs = new Path(file._1).getFileSystem(newSpark.sparkContext.hadoopConfiguration)
        FileUtil.copy(fs, new Path(file._1), fs, new Path(file._2), false, true, newSpark.sparkContext.hadoopConfiguration)
    }.foreach(print(_))
   */

    val serConfig = new ConfigSerDeser(rdd.sparkContext.hadoopConfiguration)
    val r = rdd.mapPartitions(value => {
      //val newSpark = SparkSession.builder().getOrCreate()
      var result = ArrayBuffer[Boolean]()
      while (value.hasNext) {
        val index = value.next()
        val fs = new Path(index._1).getFileSystem(serConfig.get())

        val flag = FileUtil.copy(fs, new Path(index._1), fs, new Path(index._2), false, serConfig.get())
        if (!flag)
          filesFailed.add(1)
        result.append(flag)

      }
      result.iterator
    }).foreach(print(_))

    //println(r)

    //val failed_task = final_result.filter(!_).count()
    if (filesFailed.value > 0 && !config.options.ignoreErrors)
      throw new RuntimeException("tasks failed")


  }

  private def checkDirectories(sourcePath: Path, sourcePathRoot: Path, targetPath: Path, sparkContext: SparkContext,
                               fileList: ArrayBuffer[(Path, Path)], folderList: ArrayBuffer[FileStatus]) {
    val fs = sourcePath.getFileSystem(sparkContext.hadoopConfiguration)

    val files = fs.listStatus(sourcePath)

    files.map {
      file =>
        val subPath = file.getPath.toString.split(sourcePathRoot.toString)(1)
        //println(subPath)
        val pathToCreate = targetPath.toString + subPath
        //print(pathToCreate)

        if (file.isDirectory) {
          //print(s" [$file] is directory type!")
          folderList.append(file)

          fs.mkdirs(new Path(pathToCreate))
          checkDirectories(file.getPath, sourcePathRoot, targetPath, sparkContext, fileList, folderList)
        }
        else if (file.isFile) {
          fileList.append((file.getPath, new Path(pathToCreate)))
          //print(s" [$file] is file type!")
        }

    }

  }


}