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
    public class BitConvertTests
    {
       

        [TestMethod()]
        public void ConvertToTest()
        {
            BitConvert bv = new BitConvert() { Index = 1 };
            Assert.IsTrue(Convert.ToBoolean(bv.ConvertTo(1))==false);
            Assert.IsTrue(Convert.ToBoolean(bv.ConvertTo(2)) == true);
            string bval = bv.SaveToString();
            IValueConvert bv2 = new BitConvert();
            bv2 =  bv2.LoadFromString(bval);
            Assert.IsTrue(bv.Index == (bv2 as BitConvert).Index);
        }

        
    }
}