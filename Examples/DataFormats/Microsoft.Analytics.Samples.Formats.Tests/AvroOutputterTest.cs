using System.Collections.Generic;
using System.IO;
using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Samples.Formats.ApacheAvro;
using Microsoft.Analytics.UnitTest;
using Microsoft.Analytics.Types.Sql;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Analytics.Samples.Formats.Tests
{

    [TestClass]
    public class AvroOutputterTest
    {
        [TestMethod]
        public void TestF()
        {
            var columnA = new USqlColumn<string>("A");
            var columnB = new USqlColumn<string>("B");
            var columns = new List<IColumn> { columnA, columnB };
            var schema = new USqlSchema(columns);
            var row1 = new USqlRow(schema, new object[] { "1111", new SqlArray<string> { } });
            var row2 = new USqlRow(schema, new object[] { "1111", null });
            var row3 = new USqlRow(schema, new object[] { "2222", new SqlArray<string>(new[] { "a" }) });

            using (var f = File.Create(@"C:\code\notebooks\data\avro\tmp-avrooutputter\avrooutputtertest1.avro"))
            {
                var sw = new USqlStreamWriter(f);

                var outputter = new AvroOutputter(@"{""type"":""record"",""name"":""Microsoft.Streaming.Avro.GenericFromIRecord0"",""fields"":[{""name"":""A"",""type"":[""null"",""string""]},{""name"": ""B"", ""type"": [""null"", {""type"" : ""array"", ""items"" : ""string""}]}]}");
                outputter.Output(row1, sw);
                outputter.Output(row2, sw);
                outputter.Output(row3, sw);
                outputter.Close();
                sw.Close();
            }

        }
    }
}
