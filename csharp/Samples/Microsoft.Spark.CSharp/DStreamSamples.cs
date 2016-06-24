// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization.Formatters.Binary;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;
using Microsoft.Spark.CSharp.Samples;
using Microsoft.Spark.CSharp.Interop.Ipc;

using NUnit.Framework;

namespace Microsoft.Spark.CSharp
{
    class DStreamSamples
    {
        private static int count;
        private static bool stopFileServer;
        private static void StartFileServer(StreamingContext ssc, string directory, string pattern, int loops = 1)
        {
            string testDir = Path.Combine(directory, "test");
            if (!Directory.Exists(testDir))
                Directory.CreateDirectory(testDir);

            stopFileServer = false;

            string[] files = Directory.GetFiles(directory, pattern);

            Task.Run(() =>
            {
                int loop = 0;
                while (!stopFileServer)
                {
                    if (loop++ < loops)
                    {
                        DateTime now = DateTime.Now;
                        foreach (string path in files)
                        {
                            string text = File.ReadAllText(path);
                            File.WriteAllText(testDir + "\\" + now.ToBinary() + "_" + Path.GetFileName(path), text);
                        }
                    }
                    System.Threading.Thread.Sleep(200);
                }

                ssc.Stop();
            });
            
            System.Threading.Thread.Sleep(1);
        }

        [Sample("experimental")]
        internal static void DStreamTextFileSample()
        {
            count = 0;

            string directory = SparkCLRSamples.Configuration.SampleDataLocation;
            string checkpointPath = Path.Combine(directory, "checkpoint");

            SparkContext sc = SparkCLRSamples.SparkContext;
            var b = sc.Broadcast<int>(0);

            StreamingContext ssc = StreamingContext.GetOrCreate(checkpointPath,
                () =>
                {

                    StreamingContext context = new StreamingContext(sc, 2);
                    context.Checkpoint(checkpointPath);

                    var lines = context.TextFileStream(Path.Combine(directory, "test"));
                    lines = context.Union(lines, lines);
                    var words = lines.FlatMap(l => l.Split(' '));
                    var pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));

                    // since operations like ReduceByKey, Join and UpdateStateByKey are
                    // separate dstream transformations defined in CSharpDStream.scala
                    // an extra CSharpRDD is introduced in between these operations
                    var wordCounts = pairs.ReduceByKey((x, y) => x + y);
                    var join = wordCounts.Window(2, 2).Join(wordCounts, 2);
                    var state = join.UpdateStateByKey<string, Tuple<int, int>, int>(new UpdateStateHelper(b).Execute);

                    state.ForeachRDD((time, rdd) =>
                    {
                        // there's chance rdd.Take conflicts with ssc.Stop
                        if (stopFileServer)
                            return;

                        object[] taken = rdd.Take(10);
                        Console.WriteLine("-------------------------------------------");
                        Console.WriteLine("Time: {0}", time);
                        Console.WriteLine("-------------------------------------------");
                        foreach (object record in taken)
                        {
                            Console.WriteLine(record);
                            
                            var countByWord = (KeyValuePair<string, int>)record;
                            Assert.AreEqual(countByWord.Value, countByWord.Key == "The" || countByWord.Key == "lazy" || countByWord.Key == "dog" ? 92 : 88);
                        }
                        Console.WriteLine();

                        stopFileServer = true;
                    });

                    return context;
                });

            StartFileServer(ssc, directory, "words.txt");

            ssc.Start();

            ssc.AwaitTermination();
        }

        private static string brokers = ConfigurationManager.AppSettings["KafkaTestBrokers"] ?? "127.0.0.1:9092";
        private static string topic = ConfigurationManager.AppSettings["KafkaTestTopic"] ?? "test";
        // expected partitions
        private static int partitions = int.Parse(ConfigurationManager.AppSettings["KafkaTestPartitions"] ?? "10");
        // total message count
        private static uint  messages = uint.Parse(ConfigurationManager.AppSettings["KafkaMessageCount"] ?? "100");

        /// <summary>
        /// start a local kafka service and create a 'test' topic with some data before running this sample
        /// e.g. create 2 partitions with 100 messages which will be repartitioned into 10 RDD partitions
        /// TODO: automate kafka service
        /// </summary>
        [Sample("experimental")]
        internal static void DStreamDirectKafkaWithRepartitionSample()
        {
            count = 0;

            string directory = SparkCLRSamples.Configuration.SampleDataLocation;
            string checkpointPath = Path.Combine(directory, "checkpoint");

            StreamingContext ssc = StreamingContext.GetOrCreate(checkpointPath,
                () =>
                {
                    SparkContext sc = SparkCLRSamples.SparkContext;
                    StreamingContext context = new StreamingContext(sc, 2);
                    context.Checkpoint(checkpointPath);

                    var kafkaParams = new Dictionary<string, string> {
                        {"metadata.broker.list", brokers},
                        {"auto.offset.reset", "smallest"}
                    };

                    var dstream = KafkaUtils.CreateDirectStreamWithRepartition(context, new List<string> { topic }, kafkaParams, new Dictionary<string, long>(), partitions);

                    dstream.ForeachRDD((time, rdd) => 
                        {
                            long batchCount = rdd.Count();
                            int numPartitions = rdd.GetNumPartitions();

                            Console.WriteLine("-------------------------------------------");
                            Console.WriteLine("Time: {0}", time);
                            Console.WriteLine("-------------------------------------------");
                            Console.WriteLine("Count: " + batchCount);
                            Console.WriteLine("Partitions: " + numPartitions);
                            
                            // only first batch has data and is repartitioned into 10 partitions
                            if (count++ == 0)
                            {
                                Assert.AreEqual(messages, batchCount);
                                Assert.IsTrue(numPartitions >= partitions);
                            }
                            else
                            {
                                Assert.AreEqual(0, batchCount);
                                Assert.IsTrue(numPartitions == 0);
                            }
                        });

                    return context;
                });

            ssc.Start();
            ssc.AwaitTermination();
        }

        /// <summary>
        /// A sample shows that ConstantInputDStream is an input stream that always returns the same mandatory input RDD at every batch time.
        /// </summary>
        [Sample("experimental")]
        internal static void DStreamConstantDStreamSample()
        {
            var sc = SparkCLRSamples.SparkContext;
            var ssc = new StreamingContext(sc, 2);

            const int count = 100;
            const int partitions = 2;

            // create the RDD
            var seedRDD = sc.Parallelize(Enumerable.Range(0, 100), 2);
            var dstream = new ConstantInputDStream<int>(seedRDD, ssc);

            dstream.ForeachRDD((time, rdd) =>
            {
                long batchCount = rdd.Count();
                int numPartitions = rdd.GetNumPartitions();

                Console.WriteLine("-------------------------------------------");
                Console.WriteLine("Time: {0}", time);
                Console.WriteLine("-------------------------------------------");
                Console.WriteLine("Count: " + batchCount);
                Console.WriteLine("Partitions: " + numPartitions);
                Assert.AreEqual(count, batchCount);
                Assert.AreEqual(partitions, numPartitions);
            });

            ssc.Start();
            ssc.AwaitTermination();
        }

        [Sample("experimental")]
        internal static void SinkProcessorSample()
        {
            var sc = SparkCLRSamples.SparkContext;
            var ssc = new StreamingContext(sc, 2);

            string directory = SparkCLRSamples.Configuration.SampleDataLocation;
            string checkpointPath = Path.Combine(directory, "checkpoint");
            ssc.Checkpoint(checkpointPath);

            const int partitions = 2;

            // create the RDD
            var seedRDD = sc.Parallelize(Enumerable.Range(0, 100), 2);
            var dstream = new ConstantInputDStream<int>(seedRDD, ssc);
            ssc.RegisterSinkDStream(dstream, partitions, 5, new SinkProcessorHelper().Execute);
            ssc.Start();
            ssc.AwaitTermination();
        }

        [Sample("experimental")]
        internal static void DStreamSinkProcessorSample()
        {
            count = 0;

            string directory = SparkCLRSamples.Configuration.SampleDataLocation;
            string checkpointPath = Path.Combine(directory, "checkpoint");
            int numPartitions = 2;

            SparkContext sc = SparkCLRSamples.SparkContext;

            StreamingContext ssc = StreamingContext.GetOrCreate(checkpointPath,
                () =>
                {

                    StreamingContext context = new StreamingContext(sc, 2);
                    context.Checkpoint(checkpointPath);

                    var lines = context.TextFileStream(Path.Combine(directory, "test"));
                    var words = lines.FlatMap(l => l.Split(' '));
                    var pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));
                    var groups = pairs.GroupByKey(numPartitions);
                    context.RegisterSinkDStream(groups, numPartitions, 5, new SinkProcessorHelper().Execute);
                    return context;
                });

            StartFileServer(ssc, directory, "words.txt", int.MaxValue);

            ssc.Start();

            ssc.AwaitTermination();
        }
    
    }


    // Use this helper class to test broacast variable in streaming application
    [Serializable]
    internal class UpdateStateHelper
    {
        private Broadcast<int> b;

        internal UpdateStateHelper(Broadcast<int> b)
        {
            this.b = b;
        }

        internal int Execute(IEnumerable<Tuple<int, int>> vs, int s)
        {
            int result = vs.Sum(x => x.Item1 + x.Item2) + s + b.Value;
            return result;
        }
    }

    [Serializable]
    internal class SinkProcessorHelper
    {
        internal IEnumerable<int> Execute(int partitionIndex, int previousAckedRddId, IEnumerable<KeyValuePair<int, byte[]>> input)
        {
            Console.WriteLine("SinkProcessorHelper, partitionIndex: {0}, previousAckedRddId: {1}", partitionIndex, previousAckedRddId);
            BinaryFormatter formatter = new BinaryFormatter();

            foreach (var kvp in input)
            {
                int flag = kvp.Key;
                int rddId = -1;
                switch (flag)
                {
                    case SinkDStreamProcessor.RDD_BEGIN:
                        rddId = SerDe.ToInt(kvp.Value);
                        Console.WriteLine("SinkProcessorHelper, receive RDD_BEGIN, rddId: {0}", rddId);
                        break;

                    case SinkDStreamProcessor.RDD_ELEMENT:
                        Console.WriteLine("SinkProcessorHelper, receive RDD_ELEMENT");
                        var ms = new MemoryStream(kvp.Value);
                        var obj = formatter.Deserialize(ms);
                        KeyValuePair<string, List<int>> pair = (KeyValuePair<string, List<int>>)obj;
                        Console.WriteLine("SinkProcessorHelper, key: {0}, value: [{1}]",
                            pair.Key, string.Join(",", pair.Value.ToArray()));
                        break;

                    case SinkDStreamProcessor.RDD_END:
                        rddId = SerDe.ToInt(kvp.Value);
                        Console.WriteLine("SinkProcessorHelper, receive RDD_END, rddId: {0}", rddId);
                        yield return rddId;  // ack the processed RDD
                        break;

                    default:
                        Console.WriteLine("unexpected flag: {0}");
                        break;
                }
            }
        }

    }

}
