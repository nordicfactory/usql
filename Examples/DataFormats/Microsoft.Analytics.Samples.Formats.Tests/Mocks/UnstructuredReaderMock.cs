using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Analytics.Interfaces;

namespace Microsoft.Analytics.Samples.Formats.Tests.Mocks
{
    public class UnstructuredReaderMock : IUnstructuredReader
    {
        public UnstructuredReaderMock(Stream baseStream)
        {
            BaseStream = baseStream;
        }

        public override IEnumerable<Stream> Split(params byte[] rowdelimiter)
        {
            throw new NotImplementedException();
        }

        public override Stream BaseStream { get; }
        public override long Start => 0;
        public override long Length => 0;
    }
}