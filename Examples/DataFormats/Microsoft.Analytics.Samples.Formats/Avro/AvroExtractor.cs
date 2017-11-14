// 
// Copyright (c) Microsoft and contributors.  All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// 
// See the License for the specific language governing permissions and
// limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Analytics.Interfaces;
using Avro.File;
using Avro.Generic;

namespace Microsoft.Analytics.Samples.Formats.ApacheAvro
{
    [SqlUserDefinedExtractor(AtomicFileProcessing = true)]
    public class AvroExtractor : IExtractor
    {
        private readonly string _avroSchema;
        private readonly bool _mapToInternalSchema;
        private readonly bool _ignoreColumnMismatches;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="avroSchema">Avro schema. Used when mapToInternalSchema is false, or when there is no internal avro schema in the file.</param>
        /// <param name="mapToInternalSchema">Set to true to use the schema in the avro file instead of the avroSchema parameter</param>
        /// <param name="ignoreColumnMismatches">Set to true to ignore column type and name mismatches.</param>
        public AvroExtractor(string avroSchema, bool mapToInternalSchema = false, bool ignoreColumnMismatches = false)
        {
            _avroSchema = avroSchema;
            _mapToInternalSchema = mapToInternalSchema;
            _ignoreColumnMismatches = ignoreColumnMismatches;
        }

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            Avro.Schema avschema = null;

            if (!string.IsNullOrWhiteSpace(_avroSchema))
            {
                avschema = Avro.Schema.Parse(_avroSchema);
            }

            IFileReader<GenericRecord> fileReader = null;
            using (var stream = new UnstructuredReaderAvroWrapper(input))
            {
                var foundSchema = false;

                if (_mapToInternalSchema)
                {
                    fileReader = DataFileReader<GenericRecord>.OpenReader(stream);
                    var schema = fileReader.GetSchema();

                    foundSchema = schema != null;
                }

                if (!foundSchema)
                {
                    stream.Position = 0;
                    fileReader = DataFileReader<GenericRecord>.OpenReader(stream, avschema);
                }

                while (fileReader?.HasNext() == true)
                {
                    var avroRecord = fileReader.Next();

                    foreach (var column in output.Schema)
                    {
                        if (avroRecord.TryGetValue(column.Name, out var obj))
                        {
                            if (column.Type.IsInstanceOfType(obj))
                            {
                                output.Set(column.Name, obj);
                            }
                            else
                            {
                                if (obj == null || _ignoreColumnMismatches)
                                    output.Set(column.Name, column.DefaultValue);
                                else 
                                    throw new Exception($"Column type mismatch. Output column {column.Name} of type {column.Type} is not an instance of avro file type {obj.GetType()}");
                            }
                        }
                        else
                        {
                            if (_ignoreColumnMismatches)
                            {
                                output.Set(column.Name, column.DefaultValue);
                            }
                            else
                            {
                                var fieldsString = string.Join(", ", avroRecord.Schema.Fields.Select(field => field.Name));
                                throw new Exception($"Column mismatch. Output schema column {column.Name} does not exist in avro schema fields: [{fieldsString}]");
                            }
                        }

                    }

                    yield return output.AsReadOnly();
                }
            }
        }
    }
}
