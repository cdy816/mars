using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Tests
{
    [TestClass()]
    public class ExpressFilterTests
    {
        [TestMethod()]
        public void FromStringTest()
        {
            var ef = new ExpressFilter().FromString("(a>10 and b<5) or (c>20)");
            Assert.IsNotNull(ef.ToString());

            var sef = new SqlExpress().FromString("select count(a),sum(b),c from mars where (a>10 and b<5) or (c>20)");
            Assert.IsNotNull(sef.ToString());
        }
    }
}
