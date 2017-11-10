using System;
using System.IO;
using System.Linq;
using Microsoft.Analytics.Samples.Formats.ApacheAvro;
using Microsoft.Analytics.Samples.Formats.Tests.Mocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Analytics.Samples.Formats.Tests
{
    [TestClass]
    public class UnstructuredReaderAvroWrapperTest
    {

        [TestMethod]
        public void ReadAndSeekTest()
        {
            var bufferSize = 4;

            var arr = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            var s = new MemoryStream(arr);
            var r = new UnstructuredReaderMock(s);

            var rw = new UnstructuredReaderAvroWrapper(r, bufferSize);

            var bs = new byte[2];
            var bytesRead = rw.Read(bs, 0, 2);
            Assert.AreEqual(2, bytesRead);
            AssertEqual(arr.Take(2).ToArray(), bs);

            rw.Position = 0;
            var bs2 = new byte[5];
            var bytesRead2 = rw.Read(bs2, 0, 5);
            Assert.AreEqual(5, bytesRead2);
            AssertEqual(arr.Take(5).ToArray(), bs2);

            rw.Position = 4;
            var bs3 = new byte[1];
            var bytesRead3 = rw.Read(bs3, 0, 1);
            Assert.AreEqual(1, bytesRead3);
            AssertEqual(arr.Skip(4).Take(1).ToArray(), bs3);

            rw.Read(bs2, 0, 5);
            var bytesRead4 = rw.Read(bs2, 0, 1);
            Assert.AreEqual(0, bytesRead4);

            rw.Position = 8;
            bs3 = new byte[1];
            bytesRead3 = rw.Read(bs3, 0, 1);
            Assert.AreEqual(1, bytesRead3);
            AssertEqual(arr.Skip(8).Take(1).ToArray(), bs3);

        }

        [TestMethod]
        public void ReadAllBufferTest()
        {
            for (var i = 2; i < 20; i += 2)
            {
                Console.WriteLine($"BufferSize {i}");
                _ReadAllBufferTest(i);
            }
        }

        private static void _ReadAllBufferTest(int bufferSize)
        {
            var expected = new byte[] { 1, 2, 3, 4, 5 };
            var s = new MemoryStream(expected);
            var r = new UnstructuredReaderMock(s);

            var rw = new UnstructuredReaderAvroWrapper(r, bufferSize);

            var bs = new byte[15];
            var bytesRead = rw.Read(bs, 0, 5);

            Assert.AreEqual(expected.Length, bytesRead);
            AssertEqual(expected, bs);
        }

        private static void AssertEqual(byte[] expected, byte[] actual)
        {
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.AreEqual(expected[i], actual[i]);
            }
        }

    }
}
