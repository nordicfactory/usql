using System.Collections.Generic;
using System.IO;
using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Samples.Formats.ApacheAvro;
using Microsoft.Analytics.UnitTest;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Analytics.Samples.Formats.Tests
{
    
    [TestClass]
    public class AvroOutputterTest
    {
        [TestMethod]
        public void TestF()
        {
            var foo = new USqlColumn<string>("Type");
            var columns = new List<IColumn> { foo };
            var schema = new USqlSchema(columns);
            var row1 = new USqlRow(schema, new object[]{"1111"});
            var row2 = new USqlRow(schema, new object[]{"2222"});
            
            using (var f = File.Create(@"C:\code\notebooks\data\avro\tmp-avrooutputter\avrooutputtertest1.avro"))
            {
                var sw = new USqlStreamWriter(f);
                
                var outputter = new AvroOutputter(@"{""type"":""record"",""name"":""Microsoft.Streaming.Avro.GenericFromIRecord0"",""fields"":[{""name"":""Type"",""type"":[""null"",""string""]}]}");
                outputter.Output(row1, sw);
                outputter.Output(row2, sw);
                outputter.Close();
                sw.Close();
            } 

        }
    }
}
