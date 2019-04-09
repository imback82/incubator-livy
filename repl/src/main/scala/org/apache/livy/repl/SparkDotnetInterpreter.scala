/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.repl

import java.io.File
import java.util.concurrent.{CountDownLatch, Semaphore, TimeUnit}

import org.apache.commons.lang.StringEscapeUtils
import org.apache.livy.Logging
import org.apache.livy.client.common.ClientConf
import org.apache.livy.rsc.driver.SparkEntries
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.json4s.JsonDSL._
import org.json4s._

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe

private case class RequestResponse(content: String, error: Boolean)

// scalastyle:off println
object SparkDotnetInterpreter extends Logging {
  private var sparkEntries: SparkEntries = null

  def apply(conf: SparkConf, entries: SparkEntries): SparkDotnetInterpreter = {
    sparkEntries = entries
    val backendTimeout = sys.env.getOrElse("DOTNETBACKEND_TIMEOUT", "120").toInt
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    val sparkDotnetBackendClass = mirror.classLoader.loadClass("org.apache.spark.api.dotnet.DotnetBackend")
    val backendInstance = sparkDotnetBackendClass.getDeclaredConstructor().newInstance()

    var sparkDotnetBackendPort = 0
    val initialized = new Semaphore(0)
    // Launch a Spark .NET backend server for the .NET process to connect to
    val backendThread = new Thread("Spark .NET backend") {
      override def run(): Unit = {
        sparkDotnetBackendPort = sparkDotnetBackendClass
          .getMethod("init")
          .invoke(backendInstance, new Integer(0))
          .asInstanceOf[Int]

        initialized.release()
        sparkDotnetBackendClass.getMethod("run").invoke(backendInstance)
      }
    }

    backendThread.setDaemon(true)
    backendThread.start()
    try {
      // Wait for .NET Backend initialization to finish
      initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)
      val dotnetExec = conf
        .getOption("spark.dotnet.shell.command")
        .orElse(sys.env.get("SPARK_DOTNET_DRIVER"))
        .getOrElse("invalid")

      var packageDir = ""
      if (sys.env.getOrElse("SPARK_YARN_MODE", "") == "true" ||
          (conf.get("spark.master", "").toLowerCase == "yarn" &&
          conf.get("spark.submit.deployMode", "").toLowerCase == "cluster")) {
        packageDir = "./sparkdotnet"
      } else {
        // local mode
        val dotnetLibPath = new File(
          sys.env.getOrElse(
            "SPARK_DOTNET_PACKAGE_DIR",
            Seq(sys.env.getOrElse("SPARK_HOME", "."), "Dotnet", "lib").mkString(File.separator)))
        if (!ClientConf.TEST_MODE) {
          require(dotnetLibPath.exists(), "Cannot find spark .NET package directory.")
          packageDir = dotnetLibPath.getAbsolutePath()
        }
      }

      val builder = new ProcessBuilder(Seq(dotnetExec).asJava)
      val env = builder.environment()
      env.put("SPARK_HOME", sys.env.getOrElse("SPARK_HOME", "."))
      env.put("DOTNETBACKEND_PORT", sparkDotnetBackendPort.toString)
      env.put("SPARK_DOTNET_PACKAGE_DIR", packageDir)

      builder.redirectErrorStream(true)
      val process = builder.start()
      new SparkDotnetInterpreter(
        process,
        backendInstance,
        backendThread,
        conf.getInt("spark.livy.spark_major_version", 1))
    } catch {
      case e: Exception =>
        if (backendThread != null) {
          backendThread.interrupt()
        }
        throw e
    }
  }

  def getSparkContext(): JavaSparkContext = {
    require(sparkEntries != null)
    sparkEntries.sc()
  }

  def getSparkSession(): Object = {
    require(sparkEntries != null)
    sparkEntries.sparkSession()
  }

  def getSQLContext(): SQLContext = {
    require(sparkEntries != null)
    if (sparkEntries.hivectx() != null) sparkEntries.hivectx() else sparkEntries.sqlctx()
  }
}

class SparkDotnetInterpreter(
    process: Process,
    backendInstance: Any,
    backendThread: Thread,
    val sparkMajorVersion: Int)
    extends ProcessInterpreter(process) {
  implicit val formats = DefaultFormats

  private[this] var executionCount = 0
  override def kind: String = "sparkr"
  private[this] val isStarted = new CountDownLatch(1)

  final override protected def waitUntilReady(): Unit = {
    /*
    // Set the option to catch and ignore errors instead of halting.
    sendRequest("options(error = dump.frames)")
    if (!ClientConf.TEST_MODE) {
      // scalastyle:off line.size.limit
      sendRequest("library(SparkR)")
      sendRequest("""port <- Sys.getenv("EXISTING_SPARK_DOTNET_BACKEND_PORT", "")""")
      sendRequest("""SparkR:::connectBackend("localhost", port, 6000)""")
      sendRequest(
        """assign(".scStartTime", as.integer(Sys.time()), envir = SparkR:::.sparkREnv)""")

      sendRequest(
        """assign(".sc", SparkR:::callJStatic("org.apache.livy.repl.SparkRInterpreter", "getSparkContext"), envir = SparkR:::.sparkREnv)""")
      sendRequest("""assign("sc", get(".sc", envir = SparkR:::.sparkREnv), envir=.GlobalEnv)""")

      if (sparkMajorVersion >= 2) {
        sendRequest(
          """assign(".sparkRsession", SparkR:::callJStatic("org.apache.livy.repl.SparkRInterpreter", "getSparkSession"), envir = SparkR:::.sparkREnv)""")
        sendRequest(
          """assign("spark", get(".sparkRsession", envir = SparkR:::.sparkREnv), envir=.GlobalEnv)""")
      }

      sendRequest(
        """assign(".sqlc", SparkR:::callJStatic("org.apache.livy.repl.SparkRInterpreter", "getSQLContext"), envir = SparkR:::.sparkREnv)""")
      sendRequest(
        """assign("sqlContext", get(".sqlc", envir = SparkR:::.sparkREnv), envir = .GlobalEnv)""")
      // scalastyle:on line.size.limit
    }
    */

    isStarted.countDown()
    executionCount = 0
  }

  override protected def sendExecuteRequest(command: String): Interpreter.ExecuteResponse = {
    isStarted.await()
    val code = command

    try {
      val response = sendRequest(code)

      if (response.error) {
        Interpreter.ExecuteError("Error", response.content)
      } else {
        val content: JObject = TEXT_PLAIN -> response.content
        Interpreter.ExecuteSuccess(content)
      }

    } catch {
      case e: Error =>
        Interpreter.ExecuteError("Error", e.output)
      case e: Exited =>
        Interpreter.ExecuteAborted(e.getMessage)
    }
  }

  private def sendRequest(code: String): RequestResponse = {
    stdin.println(s"""${StringEscapeUtils.escapeJava(code)}""".stripMargin)
    stdin.flush()

    val output = stdout.readLine()
    RequestResponse(output, error = false)
  }

  override protected def sendShutdownRequest() = {
    stdin.println("q()")
    stdin.flush()

    while (stdout.readLine() != null) {}
  }

  override def close(): Unit = {
    try {
      val closeMethod = backendInstance.getClass().getMethod("close")
      closeMethod.setAccessible(true)
      closeMethod.invoke(backendInstance)

      backendThread.interrupt()
      backendThread.join()
    } finally {
      super.close()
    }
  }

  private class Exited(val output: String) extends Exception {}
  private class Error(val output: String) extends Exception {}
}
// scalastyle:on println
