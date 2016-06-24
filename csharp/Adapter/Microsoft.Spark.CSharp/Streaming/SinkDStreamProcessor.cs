// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;

using Microsoft.Spark.CSharp;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Streaming
{
    /// <summary>
    /// This class is used to wrap user code to interact with JVM sink processor
    /// </summary>
    [Serializable]
    public class SinkDStreamProcessor
    {

        /// <summary>
        /// flag to indicate that a new RDD iterator begins
        /// </summary>
        public const int RDD_BEGIN = 1;

        /// <summary>
        /// flag to indicate that a RDD element
        /// </summary>
        public const int RDD_ELEMENT = 2;

        /// <summary>
        /// flag to indicate that the data of RDD has been sent completely
        /// </summary>
        public const int RDD_END = 3;

        private readonly Func<int, int, IEnumerable<KeyValuePair<int, byte[]>>, IEnumerable<int>> userFunc;

        [NonSerialized]
        private static ILoggerService logger = null;
        private ILoggerService Logger
        {
            get
            {
                if (logger == null)
                {
                    logger = LoggerServiceFactory.GetLogger(typeof(SinkDStreamProcessor));
                }
                return logger;
            }
        }

        internal SinkDStreamProcessor(Func<int, int, IEnumerable<KeyValuePair<int, byte[]>>, IEnumerable<int>> userFunc)
        {
            this.userFunc = userFunc;
        }

        private IEnumerable<KeyValuePair<int, byte[]>> GetInputEnumerator(NetworkStream networkStream)
        {
            byte[] key = null;
            byte[] value = null;
            int flag = 0;

            while (true)
            {
                int keyLength = SerDe.ReadInt(networkStream);
                if (keyLength > 0)
                {
                    key = SerDe.ReadBytes(networkStream, keyLength);
                    flag = SerDe.ToInt(key);
                }
                else
                {
                    throw new Exception(string.Format("unexpected keyLength: {0}", keyLength));
                }

                int valueLength = SerDe.ReadInt(networkStream);
                if (valueLength > 0)
                {
                    value = SerDe.ReadBytes(networkStream, valueLength);
                }
                else if (valueLength == (int)SpecialLengths.NULL)
                {
                    value = null;
                }
                else
                {
                    throw new Exception(string.Format("unexpected valueLength: {0}", valueLength));
                }

                yield return new KeyValuePair<int, byte[]>(flag, value);
            }
        }

        internal IEnumerable<byte[]> Execute(int partitionIndex, IEnumerable<byte[]> inputValues)
        {
            var inputList = inputValues.ToList();
            if (inputList.Count != 2)
            {
                throw new Exception(string.Format("unexpected inputValues count: {0}", inputList.Count));
            }

            int previousAckedRddId = SerDe.ToInt(inputList[0]);
            int serverPort = SerDe.ToInt(inputList[1]);
            Logger.LogInfo(string.Format("previousAckedRddId: {0}, serverPort: {1}", previousAckedRddId, serverPort));

            Socket sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            sock.Connect(IPAddress.Loopback, serverPort);
            Logger.LogInfo("connected to JVM server!");

            using (NetworkStream networkStream = new NetworkStream(sock))
            {
                var inputEnumerator = GetInputEnumerator(networkStream);
                var outputEnumerator = userFunc(partitionIndex, previousAckedRddId, inputEnumerator);
                foreach (var rddId in outputEnumerator)
                {
                    Logger.LogInfo("send ack rdd: {0} to JVM", rddId);
                    SerDe.Write(networkStream, rddId);
                    networkStream.Flush();
                }
            }

            yield break;
        }
    }
}
