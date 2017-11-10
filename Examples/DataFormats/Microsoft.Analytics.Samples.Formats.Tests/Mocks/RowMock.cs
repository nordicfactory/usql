using System;
using Microsoft.Analytics.Interfaces;

namespace Microsoft.Analytics.Samples.Formats.Tests.Mocks
{
    public class RowMock : IRow
    {
        public RowMock(ISchema schema)
        {
            Schema = schema;
        }

        public override T Get<T>(int index)
        {
            throw new NotImplementedException();
        }

        public override T Get<T>(string name)
        {
            throw new NotImplementedException();
        }

        public override IUpdatableRow AsUpdatable()
        {
            throw new NotImplementedException();
        }

        public override ISchema Schema { get; }
    }
}