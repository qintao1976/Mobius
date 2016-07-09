/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.streaming.api.csharp

import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

import scala.reflect.ClassTag
import scala.collection.mutable

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.hadoop.fs.Path
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, DStream}
import org.apache.spark.{SparkException, Partition, Logging}
import org.apache.spark.streaming.{Time, Duration, StreamingContext}
import org.apache.spark.streaming.scheduler.Job
import org.apache.spark.util.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rpc._

private [csharp] case class SinkProcessorTrackingInfo(
    partitionIndex: Int,
    var host: String,
    var executorId: String,
    var endpoint: Option[RpcEndpointRef],
    var ackedRddId: Int)

private [csharp] case class RddTrackingInfo[T: ClassTag] (
    rdd: RDD[T],
    rddBinary: Broadcast[Array[Byte]])

private [csharp] case class RddAndPartition(
    partition: Partition,
    rddBinary: Broadcast[Array[Byte]])

private[csharp] sealed trait SinkTrackerMessage

private [csharp] case class AddRdd(rdd: RDD[_]) extends SinkTrackerMessage

private[csharp] case class RegisterSink(
    partitionIndex: Int,
    host: String,
    executorId: String,
    sinkEndpoint: RpcEndpointRef
  ) extends SinkTrackerMessage

private[csharp] case class RegisterSinkResponse(
    partitionIndex: Int,
    previousAckedRddId: Int
  ) extends SinkTrackerMessage

private[csharp] case class AddRddPartitions(
    rddAndPartitions: List[RddAndPartition]
  ) extends SinkTrackerMessage

private[csharp] case class AckRdd(
    partitionIndex: Int,
    rddId: Int
  ) extends SinkTrackerMessage

/**
  This is the outputDStream of stream graph. It will
  1. Create multiple long running tasks called SinkProcessor in executors.
  2. In the override generateJob() method, it will
  (a) Get RDDs from parent DStream, materialize and checkpoint them.
  (b) Send AddRddPartitions message to SinkProcessors.
  (c) Receive AckRdd message from SinkProcessors, and save this info to HDFS.
 */
private [csharp] class CSharpSinkDStream[T: ClassTag](
    @transient val _ssc: StreamingContext,
    val parent: DStream[T],
    val numPartitions: Int,
    val ackedRddRetainNum: Int,  // retain some extra RDDs even after it is acked for replay
    val command: Array[Byte],
    val envVars: JHashMap[String, String],
    val cSharpIncludes: JArrayList[String],
    val cSharpWorkerExecutable: String,
    val unUsedVersionIdentifier: String,
    val broadcastVars: JArrayList[Broadcast[PythonBroadcast]])
  extends DStream[Unit](_ssc) with Logging {

  // For simpliciy, only support one CSharpSinkDStream in each streamingContext
  private val CheckPointFile = "CSharpSinkDStreamSnapshot.data"
  private val CheckPointBackupFile = "CSharpSinkDStreamSnapshot.bak"
  private val CheckPointTmpFile = "CSharpSinkDStreamSnapshot.tmp"
  @transient private var checkpointDir: String = context.sparkContext.checkpointDir.orNull(null)

  @transient private var sinkProcessorTrackingInfos:
      mutable.HashMap[Int, SinkProcessorTrackingInfo] = null
  @transient protected var addressToPartitionIndex: mutable.HashMap[RpcAddress, Int] = null
  @transient private var ackedRddTrackingInfos: mutable.ListBuffer[RddTrackingInfo[T]] = null
  @transient private var unackedRddTrackingInfos: mutable.ListBuffer[RddTrackingInfo[T]] = null

  @transient private var serializer: org.apache.spark.serializer.SerializerInstance = null
  @transient private var endpoint: RpcEndpointRef = null
  @transient private var jsonMapper: ObjectMapper = null

  protected[streaming] override val checkpointData = new CSharpSinkDStreamCheckpointData

  private def initialize(): Unit = {
    addressToPartitionIndex = mutable.HashMap.empty
    ackedRddTrackingInfos = mutable.ListBuffer.empty
    unackedRddTrackingInfos = mutable.ListBuffer.empty
    sinkProcessorTrackingInfos = mutable.HashMap.empty
    for (i <- 0 until numPartitions) {
      val sinkTrackingInfo = SinkProcessorTrackingInfo(i, null, null, None, -1)
      sinkProcessorTrackingInfos.put(i, sinkTrackingInfo)
    }

    serializer = context.env.closureSerializer.newInstance()
    jsonMapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .registerModule(DefaultScalaModule)
    endpoint = context.env.rpcEnv.setupEndpoint(
      "SinkProcessorTracker", new SinkProcessorTrackerEndpoint(context.env.rpcEnv))
  }

  private def deleteFile(file: String): Unit = {
    val path = new Path(file)
    val fs = path.getFileSystem(context.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        logWarning(s"Error deleting ${path.toString}")
      }
    }
  }

  private def restoreRddFromCheckpoint(checkpointPath: String): Option[RDD[T]] = {
    val path = new Path(checkpointPath)
    val fs = path.getFileSystem(context.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      Some(context.sparkContext.checkpointFile[T](checkpointPath))
    } else {
      None
    }
  }

  private def takeSnapshot(): CSharpSinkDStreamSnapshot = {
    val rddAckInfos = mutable.Map[String, Int]()
    val rddCheckpointFiles = mutable.ListBuffer[String]()

    sinkProcessorTrackingInfos.foreach({ case (_, sinkTrackingInfo) =>
      rddAckInfos += (sinkTrackingInfo.partitionIndex.toString -> sinkTrackingInfo.ackedRddId)
    })

    (ackedRddTrackingInfos.toList ++ unackedRddTrackingInfos.toList).foreach({
      case RddTrackingInfo(rdd, _) => rddCheckpointFiles ++= rdd.getCheckpointFile
    })

    new CSharpSinkDStreamSnapshot(Map(rddAckInfos.toList: _*), rddCheckpointFiles.toList)
  }

  private def saveSnapshot(): Unit = {
    val snapshot = takeSnapshot()
    // using json format instead of saving Java object directly can improve backforward compability
    val bytes = jsonMapper.writeValueAsBytes(snapshot)
    val tempFile = new Path(checkpointDir, CheckPointTmpFile)
    val fs = tempFile.getFileSystem(context.sparkContext.hadoopConfiguration)
    // Write snapshot to temp file
    if (fs.exists(tempFile)) {
      fs.delete(tempFile, true)   // just in case it exists
    }
    val fos = fs.create(tempFile)
    Utils.tryWithSafeFinally {
      fos.write(bytes)
    } {
      fos.close()
    }
    val checkpointFile = new Path(checkpointDir, CheckPointFile)
    if (fs.exists(checkpointFile)) {
      val backupFile = new Path(checkpointDir, CheckPointBackupFile)
      if (fs.exists(backupFile)) {
        fs.delete(backupFile, true)
      }
      if (!fs.rename(checkpointFile, backupFile)) {
        logWarning("Could not rename " + checkpointFile + " to " + backupFile)
      }
    }
    // Rename temp file to the final checkpoint file
    if (!fs.rename(tempFile, checkpointFile)) {
      logWarning("Could not rename " + tempFile + " to " + checkpointFile)
    }
  }

  private def updateRddTrackStatus(): Unit = {
    var stop = false
    var shouldSaveSnapshot = false
    while (!stop && unackedRddTrackingInfos.nonEmpty) {
      val rddTrackingInfo = unackedRddTrackingInfos.head
      val allAcked = sinkProcessorTrackingInfos.forall({
        case (_, sinkTrackingInfo) =>
          sinkTrackingInfo.ackedRddId >= rddTrackingInfo.rdd.id
      })
      if (allAcked) {
        // if all sink processors have acked this RDD, then move it from unacked to acked queue
        unackedRddTrackingInfos.remove(0)
        ackedRddTrackingInfos += rddTrackingInfo
        shouldSaveSnapshot = true
      } else {
        stop = true
      }
    }

    val ackedRddSize = ackedRddTrackingInfos.length
    var deleteFiles = new mutable.ListBuffer[String]()
    if (ackedRddSize > ackedRddRetainNum) {
      val deleteCount = ackedRddSize - ackedRddRetainNum
      ackedRddTrackingInfos.take(deleteCount).foreach( {
        case RddTrackingInfo(rdd, _) =>
          rdd.unpersist()
          deleteFiles ++= rdd.getCheckpointFile
      })
      ackedRddTrackingInfos.remove(0, deleteCount)
      shouldSaveSnapshot = true
    }

    // to avoid corrupted files, first save snapshot then delete files
    if (shouldSaveSnapshot) {
      saveSnapshot()
    }
    deleteFiles.foreach(deleteFile)
  }

  private def trackRdd(rdd: RDD[T]): RddTrackingInfo[T] = {
    val rddBytes = serializer.serialize(rdd).array()
    val rddBinary = context.sparkContext.broadcast(rddBytes)
    val rddTrackingInfo = RddTrackingInfo[T](rdd, rddBinary)
    unackedRddTrackingInfos += rddTrackingInfo
    rddTrackingInfo
  }

  private def restoreSnapshot(): Unit = {
    val checkpointFile = new Path(checkpointDir, CheckPointFile)
    val backupFile = new Path(checkpointDir, CheckPointBackupFile)
    val fs = checkpointFile.getFileSystem(context.sparkContext.hadoopConfiguration)
    val file = {
      if (fs.exists(checkpointFile)) {
        checkpointFile
      } else if (fs.exists(backupFile)) {
        backupFile
      } else {
        null
      }
    }
    if (file != null) {
      logInfo(s"use file $file to restore snapshot")
      val fis = fs.open(file)
      Utils.tryWithSafeFinally {
        val snapshot = jsonMapper.readValue(fis, classOf[CSharpSinkDStreamSnapshot])
        for ((partitionIndex, ackedRddId) <- snapshot.rddAckInfos) {
          val sinkTrackingInfo = sinkProcessorTrackingInfos(partitionIndex.toInt)
          sinkTrackingInfo.ackedRddId = ackedRddId
        }
        for (rddCheckpointFile <- snapshot.rddCheckpointFiles) {
          val rddOption = restoreRddFromCheckpoint(rddCheckpointFile)
          if (rddOption.isDefined) {
            trackRdd(rddOption.get)
          }
        }
      } {
        fis.close()
      }
      updateRddTrackStatus()
    }
  }

  private def start(): Unit = {
    logInfo(s"CSharpSinkDStream start ...")
    initialize()
    restoreSnapshot()
    launchSinkProcessors()
  }

  start()

  private def addInputRdd(inputRdd: RDD[T]): Unit = {
    // clone the RDD before the inputRdd will be removed automatially by Spark
    val rdd = context.sparkContext.union(inputRdd)
    // materialize and checkpoint the RDD
    rdd.cache()
    rdd.checkpoint()
    context.sparkContext.setCallSite(s"materialize RDD[$rdd.id]")
    context.sparkContext.runJob(rdd, (iterator: Iterator[T]) => {})
    endpoint.send(AddRdd(rdd))
  }

  private def launchSinkProcessors(): Unit = {
    // use local variable to avoid "Task not serializable" exception
    val command_ = command
    val envVars_ = envVars
    val cSharpIncludes_ = cSharpIncludes
    val cSharpWorkerExecutable_ = cSharpWorkerExecutable
    val unUsedVersionIdentifier_ = unUsedVersionIdentifier
    val broadcastVars_ = broadcastVars
    val startSinkProcessorFunc: Iterator[Long] => Unit =
      (iterator: Iterator[Long]) => {
        if (!iterator.hasNext) {
          throw new SparkException(
            "Could not start SinkProcessor as partitionIndex not found")
        }
        val partitionIndex = iterator.next()
        assert(!iterator.hasNext)
        val sinkProcessor = new SinkProcessor(partitionIndex.toInt, command_, envVars_,
          cSharpIncludes_, cSharpWorkerExecutable_, unUsedVersionIdentifier_, broadcastVars_)
        sinkProcessor.start()
      }
    val sinkProcessorRdd = context.sparkContext.range(0, numPartitions.toLong, 1, numPartitions)
    sinkProcessorRdd.setName(s"SinkProcessor")
    context.sparkContext.setJobDescription(s"Streaming job running SinkProcessor")
    // TODO, how to handle falure and restart sink processor?
    val future = context.sparkContext.submitJob[Long, Unit, Unit](
        sinkProcessorRdd, startSinkProcessorFunc, 0 until numPartitions, (_, _) => Unit, ())
  }

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Unit]] = None

  override def generateJob(time: Time): Option[Job] = {
    parent.getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => createRDDWithLocalProperties(time, true) {
          addInputRdd(rdd)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
  }

  /** RpcEndpoint to receive messages from the SinkProcessors. */
  private class SinkProcessorTrackerEndpoint(override val rpcEnv: RpcEnv)
        extends ThreadSafeRpcEndpoint {

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterSink(partitionIndex, host, executorId, sinkEndpoint) =>
        logInfo(s"receive register msg, partitionIndex: $partitionIndex," +
          s" host: $host, executorId: $executorId")
        if (partitionIndex >= numPartitions) {
          logWarning(s"partitionIndex $partitionIndex > numPartitions $numPartitions")
        } else {
          addressToPartitionIndex(sinkEndpoint.address) = partitionIndex
          val sinkTrackingInfo = sinkProcessorTrackingInfos(partitionIndex)
          sinkTrackingInfo.host = host
          sinkTrackingInfo.executorId = executorId
          sinkTrackingInfo.endpoint = Some(sinkEndpoint)
          context.reply(RegisterSinkResponse(partitionIndex, sinkTrackingInfo.ackedRddId))
          val msg = AddRddPartitions(
            (ackedRddTrackingInfos.toList ++ unackedRddTrackingInfos.toList).flatMap {
              case RddTrackingInfo(rdd, rddBinary) =>
                if (partitionIndex < rdd.partitions.length) {
                  Some(RddAndPartition(rdd.partitions(partitionIndex), rddBinary))
                } else {
                  None
                }
            }
          )
          sinkEndpoint.send(msg)
        }
    }

    override def receive: PartialFunction[Any, Unit] = {
      case AckRdd(partitionIndex, rddId) =>
        logInfo(s"receive AckRdd msg, partitionIndex: $partitionIndex, rddId: $rddId")
        val sinkTrackingInfo = sinkProcessorTrackingInfos(partitionIndex)
        sinkTrackingInfo.ackedRddId = rddId
        updateRddTrackStatus()

      case AddRdd(newRdd) =>
        val rdd = newRdd.asInstanceOf[RDD[T]]
        logInfo(s"receive AddRdd[${rdd.id}]")
        val rddTrackingInfo = trackRdd(rdd)
        for ((partitionIndex, sinkTrackingInfo) <- sinkProcessorTrackingInfos) {
          if (sinkTrackingInfo.endpoint.isDefined && partitionIndex < rdd.partitions.length) {
            val msg = AddRddPartitions(
              List(RddAndPartition(rdd.partitions(partitionIndex), rddTrackingInfo.rddBinary)))
            sinkTrackingInfo.endpoint.get.send(msg)
          }
        }
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      if (addressToPartitionIndex.contains(remoteAddress)) {
        if (addressToPartitionIndex.contains(remoteAddress)) {
          val partitionIndex = addressToPartitionIndex(remoteAddress)
          val sinkTrackingInfo = sinkProcessorTrackingInfos(partitionIndex)
          sinkTrackingInfo.endpoint = None
          logInfo(s"sink disconnected! partitionIndex: $partitionIndex, host: ${sinkTrackingInfo.host}, " +
            s" executorId: ${sinkTrackingInfo.executorId}")
          addressToPartitionIndex.remove(remoteAddress)
        }
      }
    }
  }


  private[csharp] class CSharpSinkDStreamCheckpointData extends DStreamCheckpointData(this) {

    // need to save and restore checkpointDir because streamingContext.checkpointDir is not
    // available in the restore stage.
    private val innerCheckpointDir = context.checkpointDir

    override def update(time: Time) { }

    override def cleanup(time: Time) { }

    override def restore() {
      checkpointDir = innerCheckpointDir
      logInfo(s"Restoring ..., checkpointDir: $checkpointDir")
      start()
    }
  }
}

private class CSharpSinkDStreamSnapshot(
    val rddAckInfos: Map[String, Int],
    val rddCheckpointFiles: List[String]) {

  // Constructor with empty parameters is nessary for JSON deserialization
  def this() = this(Map(), List())
}
