using System;
using System.Collections.Generic;
using Microsoft.Analytics.Interfaces;

namespace Microsoft.Analytics.Samples.Formats.Tests.Mocks
{
    public class SchemaMock : ISchema
    {
        public override int IndexOf(string column)
        {
            throw new NotImplementedException();
        }

        public override IEnumerator<IColumn> GetEnumerator()
        {
            return new List<IColumn>().GetEnumerator();
        }

        public override int Count { get; }

        public override IColumn this[int index] => throw new NotImplementedException();

        public override IRow Defaults { get; }
    }
}