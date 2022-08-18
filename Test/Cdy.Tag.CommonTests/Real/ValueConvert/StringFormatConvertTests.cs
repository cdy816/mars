using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag.Tests
{
    [TestClass()]
    public class StringFormatConvertTests
    {
        [TestMethod()]
        public void ConvertBackToTest()
        {
            StringFormatConvert sfc = new StringFormatConvert();
            sfc.Format = "this is {0}";
            Assert.IsTrue(sfc.ConvertTo("test").ToString() == "this is test");
        }
    }
}