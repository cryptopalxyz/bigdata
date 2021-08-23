package sparkcourse.sparkdistcp

//使用FileSystem类FileStatus区分目录和文件，filter出目录，递归创建目录
//FileSystem类下吗的mkdirs


import groovy.lang.Tuple
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import sparkcourse.distcp.utils.OptionsParsing
import sparkcourse.sparkdistcp.interfaces.Logging

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

    var fileList = ArrayBuffer[(Path, Path)]()
    var folderList = ArrayBuffer[FileStatus]()

    @transient
    val fs = sourcePath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    checkDirectories(sourcePath, sourcePathRoot, targetPath, spark.sparkContext, fileList, folderList)
    val fileStringList = fileList.map((s) => (s._1.toString(), s._2.toString))

    val rdd = spark.sparkContext.makeRDD(fileStringList, config.options.maxConcurrency)

    rdd.map {
      file =>
        val fs = new Path(file._1).getFileSystem(spark.sparkContext.hadoopConfiguration)
        FileUtil.copy(fs, new Path(file._1), fs, new Path(file._2), false, spark.sparkContext.hadoopConfiguration)
    }

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
