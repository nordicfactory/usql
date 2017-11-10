using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.Analytics.Samples.Formats.ApacheAvro;
using Microsoft.Analytics.Samples.Formats.Tests.Mocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Analytics.Samples.Formats.Tests
{
    [TestClass]
    public class AvroExtractorFileTest
    {
        //[TestMethod]
        public void RealBigFileTest()
        {
            var path = "C:\\code\\notebooks\\data\\avro\\enriched-20171009-04\\-1824167840_0d754003a7694c4caf4267a0d3764150_1.avro";

            using (var s = File.OpenRead(path))
            {
                var input = new UnstructuredReaderMock(s);

                var e = new AvroExtractor("", true);
                var output = new UpdatableRowMock();
                var sw = Stopwatch.StartNew();
                var rows = e.Extract(input, output);

                Console.WriteLine($"Done. Rows: {rows.Count()} in {sw.Elapsed.TotalSeconds}s");

            }
        }

    }
}
