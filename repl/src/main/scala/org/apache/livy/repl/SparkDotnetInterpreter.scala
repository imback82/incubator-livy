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

import org.apache.livy.Logging
import org.apache.livy.client.common.ClientConf
import org.apache.livy.rsc.driver.SparkEntries
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.json4s.JsonDSL._
import org.json4s._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe

// scalastyle:off println
object SparkDotnetInterpreter extends Logging {
  private var sparkEntries: SparkEntries = null

  def apply(conf: SparkConf, entries: SparkEntries): SparkDotnetInterpreter = {
    sparkEntries = entries
    val backendTimeout = sys.env.getOrElse("DOTNETBACKEND_TIMEOUT", "120").toInt
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    val sparkDotnetBackendClass =
      mirror.classLoader.loadClass("org.apache.spark.api.dotnet.DotnetBackend")
    val backendInstance = sparkDotnetBackendClass.getDeclaredConstructor().newInstance()

    var sparkDotnetBackendPort = 0
    val initialized = new Semaphore(0)
    // Launch a Spark .NET backend server for the .NET process to connect to
    val backendThread = new Thread("Spark .NET backend") {
      override def run(): Unit = {
        sparkDotnetBackendPort = sparkDotnetBackendClass
          .getMethod("init", Integer.TYPE)
          .invoke(backendInstance, new Integer(0))
          .asInstanceOf[Int]

        warn(s"spark .NET backend listening on port $sparkDotnetBackendPort")

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

      val builder = new ProcessBuilder(dotnetExec.split(" ").toSeq.asJava)
      builder.directory(new File(sys.env.getOrElse("SPARK_DOTNET_PACKAGE_DIR", ".")))

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
    val response = readTo("> ", "lkdsajglksadjgkjasldg")
    val content = response;
    warn(s"flusing: $content")

    val dotnetLibPath = sys.env.getOrElse("SPARK_DOTNET_PACKAGE_DIR", ".")
    sendRequest("#r \"Microsoft.Spark.dll\"")
    sendRequest("using Microsoft.Spark.Sql;")
    sendRequest("var spark = SparkSession.Builder().AppName(\"livy\").GetOrCreate();")

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
    val ccode = code.stripMargin
    warn(s"About to execute '$ccode'")

    stdin.println(ccode)
    stdin.flush()

    warn(s"Finished executing '$ccode'")

    val response = readTo("> ", "lkdsajglksadjgkjasldg")

    warn(s"Done reading the output for. '$ccode'")

    response
  }

  override protected def sendShutdownRequest() = {
    stdin.println("#exit")
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

  @tailrec
  private def readTo(
      marker: String,
      errorMarker: String,
      output: StringBuilder = StringBuilder.newBuilder): RequestResponse = {
    var char = readChar(output)
    // warn(s"readTo: '" + output.toString() + "'")
    // Remove any ANSI color codes which match the pattern "\u001b\\[[0-9;]*[mG]".
    // It would be easier to do this with a regex, but unfortunately I don't see an easy way to do
    // without copying the StringBuilder into a string for each character.
    if (char == '\u001b') {
      if (readChar(output) == '[') {
        char = readDigits(output)

        if (char == 'm' || char == 'G') {
          output.delete(output.lastIndexOf('\u001b'), output.length)
        }
      }
    }

    if (output.endsWith(marker)) {
      var result = stripMarker(output.toString(), marker)

      if (result.endsWith(errorMarker + "\"")) {
        result = stripMarker(result, "\\n" + errorMarker)
        warn(s"Error result $result")
        RequestResponse(result, error = true)
      } else {
        warn(s"Good result $result")
        RequestResponse(result, error = false)
      }
    } else {
      if (output.endsWith("* ")) {
        warn("continuation block found")
        stdin.println(";")
        stdin.flush()
      }

      readTo(marker, errorMarker, output)
    }
  }

  private def stripMarker(result: String, marker: String): String = {
    result
      .replace(marker, "")
      .stripPrefix("\n")
      .stripSuffix("\n")
  }

  private def readChar(output: StringBuilder): Char = {
    val byte = stdout.read()
    if (byte == -1) {
      throw new Exited(output.toString())
    } else {
      val char = byte.toChar
      output.append(char)
      char
    }
  }

  @tailrec
  private def readDigits(output: StringBuilder): Char = {
    val byte = stdout.read()
    if (byte == -1) {
      throw new Exited(output.toString())
    }

    val char = byte.toChar

    if (('0' to '9').contains(char)) {
      output.append(char)
      readDigits(output)
    } else {
      char
    }
  }

  private class Exited(val output: String) extends Exception {}
  private class Error(val output: String) extends Exception {}
}
// scalastyle:on println
