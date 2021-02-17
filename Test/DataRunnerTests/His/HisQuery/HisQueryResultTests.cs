using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cdy.Tag;
//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/28 16:42:29.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Tests
{
    [TestClass()]
    public class HisQueryResultTests
    {
        [TestMethod()]
        public void HisQueryResultTest()
        {
            HisQueryResult<bool> res = new HisQueryResult<bool>(10);

     
            HisQueryResult<byte> res1 = new HisQueryResult<byte>(10);
           
            HisQueryResult<short> res2 = new HisQueryResult<short>(10);
            HisQueryResult<ushort> res3 = new HisQueryResult<ushort>(10);
            HisQueryResult<int> res4 = new HisQueryResult<int>(10);
            HisQueryResult<uint> res5 = new HisQueryResult<uint>(10);
            HisQueryResult<long> res6 = new HisQueryResult<long>(10);
            HisQueryResult<ulong> res7 = new HisQueryResult<ulong>(10);
            HisQueryResult<DateTime> res8 = new HisQueryResult<DateTime>(10);
            HisQueryResult<double> res9 = new HisQueryResult<double>(10);
            HisQueryResult<float> res10 = new HisQueryResult<float>(10);
            HisQueryResult<string> res11 = new HisQueryResult<string>(10);
            Assert.IsTrue(res.Length == 100);
            Assert.IsTrue(res1.Length == 100);
            Assert.IsTrue(res2.Length == 110);
            Assert.IsTrue(res3.Length == 110);
            Assert.IsTrue(res4.Length == 130);
            Assert.IsTrue(res5.Length == 130);
            Assert.IsTrue(res6.Length == 170);
            Assert.IsTrue(res7.Length == 170);
            Assert.IsTrue(res8.Length == 170);

            Assert.IsTrue(res9.Length == 170);
            Assert.IsTrue(res10.Length == 130);
            Assert.IsTrue(res11.Length == (Const.StringSize + 9) *10);
           
        }

        [TestMethod()]
        public void GetValueTest()
        {
            DateTime date = DateTime.Now;
            DateTime resd;
            byte qulity = 0;
            HisQueryResult<bool> res = new HisQueryResult<bool>(10);
            res.Add(true, date, 1);
           
            var bval = res.GetValue(0, out resd, out qulity);

            Assert.IsTrue(bval == true);
            Assert.IsTrue(resd == date);
            Assert.IsTrue(qulity == 1);
            res.Clear();

            HisQueryResult<double> res2 = new HisQueryResult<double>(10);
            res2.Add(0.24, date, 1);

            var dval = res2.GetValue(0, out resd, out qulity);

            Assert.IsTrue(dval == 0.24);
            Assert.IsTrue(resd == date);
            Assert.IsTrue(qulity == 1);

            res2.Resize(20);

            dval = res2.GetValue(0, out resd, out qulity);

            Assert.IsTrue(dval == 0.24);
            Assert.IsTrue(resd == date);
            Assert.IsTrue(qulity == 1);

            HisQueryResult<double> res3 = new HisQueryResult<double>(10);

            res2.CloneTo(res3);

            dval = res3.GetValue(0, out resd, out qulity);

            Assert.IsTrue(dval == 0.24);
            Assert.IsTrue(resd == date);
            Assert.IsTrue(qulity == 1);
        }


        [TestMethod()]
        public void ClearTest()
        {
            Assert.Fail();
        }

    }
}