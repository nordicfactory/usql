using System;
using System.Linq;
using Avro;
using Avro.File;
using Avro.Generic;
using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;

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
                var obj = input.Get<object>(x.Name);

                if (obj != null)
                {
                    var objType = obj.GetType();
                    if (objType.IsGenericType && objType.GetGenericTypeDefinition() == typeof(SqlArray<>))
                        obj = ((System.Collections.IEnumerable) obj).Cast<object>().ToArray();
                }

                record.Add(x.Name, obj);
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
