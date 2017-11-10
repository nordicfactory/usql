using System;
using Microsoft.Analytics.Interfaces;

namespace Microsoft.Analytics.Samples.Formats.Tests.Mocks
{
    public class ColumnMock : IColumn
    {
        public ColumnMock(string name, Type type, object defaultValue)
        {
            Name = name;
            Type = type;
            DefaultValue = defaultValue;
        }

        public override string Name { get; }
        public override Type Type { get; }
        public override object DefaultValue { get; }
    }
}