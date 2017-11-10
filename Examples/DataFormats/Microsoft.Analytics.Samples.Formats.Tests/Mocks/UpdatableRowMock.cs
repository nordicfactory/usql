using System;
using Microsoft.Analytics.Interfaces;

namespace Microsoft.Analytics.Samples.Formats.Tests.Mocks
{
    public class UpdatableRowMock : IUpdatableRow
    {
        public override void Set<T>(int index, T value)
        {
            throw new NotImplementedException();
        }

        public override void Set<T>(string column, T value)
        {
            throw new NotImplementedException();
        }

        public override T Get<T>(int index)
        {
            throw new NotImplementedException();
        }

        public override T Get<T>(string name)
        {
            throw new NotImplementedException();
        }

        public override IRow AsReadOnly()
        {
            return new RowMock(Schema);
        }

        public override ISchema Schema => new SchemaMock();
    }
}