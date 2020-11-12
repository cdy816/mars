using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cdy.Tag.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Common.Tests
{
    [TestClass()]
    public class IdBufferTests
    {
        [TestMethod()]
        public void SetIdTest()
        {
            IdBuffer buffer = new IdBuffer();
            buffer.SetId(1000);
            buffer.SetId(2000);
            buffer.SetId(45678);
            Assert.IsTrue(buffer.CheckId(1000));
            Assert.IsTrue(buffer.CheckId(2000));
            Assert.IsTrue(buffer.CheckId(45678));
            Assert.IsFalse(buffer.CheckId(4568));
            buffer.ClearId(45678);
            Assert.IsFalse(buffer.CheckId(45678));
        }
    }
}