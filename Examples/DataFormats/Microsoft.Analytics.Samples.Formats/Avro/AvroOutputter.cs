using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Avro;
using Avro.File;
using Avro.Generic;
using Microsoft.Analytics.Interfaces;

namespace Microsoft.Analytics.Samples.Formats.ApacheAvro
{
    [SqlUserDefinedOutputter(AtomicFileProcessing = true)]
    public class AvroOutputter : IOutputter
    {
        private IFileWriter<GenericRecord> _fileWriter;
        private readonly string _avroSchema;
        private RecordSchema _avSchema;

        public AvroOutputter(string avroSchema)
        {
            _avroSchema = avroSchema;
        }

        public override void Output(IRow input, IUnstructuredWriter output)
        {
            if (_fileWriter == null)
            {
                _avSchema = Schema.Parse(_avroSchema) as RecordSchema;
                var writer = new GenericDatumWriter<GenericRecord>(_avSchema);
                _fileWriter = DataFileWriter<GenericRecord>.OpenWriter(writer, output.BaseStream);
            }
            
            var record = new GenericRecord(_avSchema);
            
            foreach (var x in input.Schema)
            {
                record.Add(x.Name, input.Get<object>(x.Name));
            }
            
            _fileWriter.Append(record);
        }

        public override void Close()
        {
            if (_fileWriter == null) return;
            try
            {
                _fileWriter.Dispose();
            }
            catch (Exception)
            {
                //Ignore this exception, since it will always(?) happen but we still want to make sure the writer is disposed.
                //Debug.WriteLine($"Exception in writer dispose. {e.Message} {e.StackTrace}");
            }
        }
    }
}
