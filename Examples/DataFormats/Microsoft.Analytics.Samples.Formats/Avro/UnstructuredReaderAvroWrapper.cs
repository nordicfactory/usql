using System;
using System.IO;
using System.Linq;
using Microsoft.Analytics.Interfaces;

namespace Microsoft.Analytics.Samples.Formats.ApacheAvro
{
    /// <inheritdoc />
    /// <summary>
    /// Wraps a IUnstructuredReader in a way that make it possible to use the reader when parsing Avro files with the Apache Avro library.   
    /// </summary>
    public class UnstructuredReaderAvroWrapper : Stream
    {
        private readonly IUnstructuredReader _reader;

        private long _position; // external position
        private long _readerPosition;

        private readonly byte[] _tmpBuffer;
        private int _tmpBufferLength;

        private int _tmpBufferStartPosition;
        private readonly int _length;

        /// <inheritdoc />
        /// <summary>
        /// Create a new instance of UnstructuredReaderAvroWrapper
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="bufferSize">Must be a even number and at least 2x the size of a row in the avro file</param>
        public UnstructuredReaderAvroWrapper(IUnstructuredReader reader, int bufferSize = 1024*1024)
        {
            _reader = reader;
            _readerPosition = 0;
            _tmpBuffer = new byte[bufferSize];
            _tmpBufferStartPosition = 0;
            _tmpBufferLength = _reader.BaseStream.Read(_tmpBuffer, 0, _tmpBuffer.Length);
            //Console.WriteLine($"Buffer {BitConverter.ToString(_tmpBuffer.Take(_tmpBufferLength).ToArray())}");
            _readerPosition = _tmpBufferLength;
            _length = _tmpBufferLength;
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            //Console.WriteLine($"Seek {offset} {origin}");
            if (origin != SeekOrigin.Begin)
                throw new NotSupportedException();
            if (offset > _position)
                throw new NotSupportedException();

            if (offset < _tmpBufferStartPosition)
                throw new ArgumentException("New position is before the start of the internal buffer");

            _position = offset;
            return _position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            //Console.WriteLine($"Read Count:{count} Position:{_position}");
            if (count > buffer.Length)
                throw new ArgumentException($"Buffer length {buffer.Length} is smaller than count {count}");
            if (offset > 0)
                throw new NotSupportedException("Nonzero offset not supported.");

            var bytesRead = 0;
            var countRemaining = count;
            while (countRemaining > 0)
            {
                if (_position < _tmpBufferStartPosition + _tmpBufferLength)
                {
                    var srcOffset = (int)(_position - _tmpBufferStartPosition);
                    var copyCount = Math.Min(_tmpBufferLength - srcOffset, count - bytesRead);
                    Buffer.BlockCopy(_tmpBuffer, srcOffset, buffer, bytesRead, copyCount);

                    countRemaining -= copyCount;
                    bytesRead += copyCount;
                    _position += copyCount;
                }

                // need to get new data into buffer
                if (countRemaining > 0)
                {
                    var bs = new byte[_tmpBuffer.Length / 2];
                    var bytesFromStream = _reader.BaseStream.Read(bs, 0, bs.Length);
                    _readerPosition += bytesFromStream;

                    if (bytesFromStream == 0)
                        break;
                    
                    // shift tmpbuffer to the left
                    Buffer.BlockCopy(_tmpBuffer, _tmpBuffer.Length / 2, _tmpBuffer, 0, _tmpBuffer.Length / 2);
                    _tmpBufferStartPosition += _tmpBuffer.Length / 2;

                    // add new data to tmpbuffer
                    Buffer.BlockCopy(bs, 0, _tmpBuffer, _tmpBuffer.Length / 2, bytesFromStream);
                    _tmpBufferLength = _tmpBuffer.Length / 2 + bytesFromStream;
                }
            }
            //Console.WriteLine($"Read {BitConverter.ToString(buffer)} Position: {_position}");

            return bytesRead;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => _length;

        public override long Position
        {
            get => _position;
            set
            {
                //Console.WriteLine($"PositionSet Value:{value}");

                Seek(value, SeekOrigin.Begin);
            }
        }
    }
}