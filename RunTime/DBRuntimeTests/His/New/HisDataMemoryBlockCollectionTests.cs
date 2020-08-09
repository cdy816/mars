using Microsoft.VisualStudio.TestTools.UnitTesting;
using DBRuntime.His;
//==============================================================
//  Copyright (C) 2020 Chongdaoyang Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/8/8 15:22:48 .
//  Version 1.0
//  CDYWORK
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using Microsoft.VisualStudio.TestPlatform.ObjectModel;

namespace DBRuntime.His.Tests
{
    [TestClass()]
    public class HisDataMemoryBlockCollectionTests
    {
        [TestMethod()]
        public void ClearTest()
        {
            HisDataMemoryBlockCollection hdb = new HisDataMemoryBlockCollection();
            for (int i = 0; i < 1000000; i++)
            {
                hdb.AddTagAddress(i, new HisDataMemoryBlock(3300));
            }

            hdb.Clear();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            foreach(var vv in hdb.TagAddress.Values)
            {
                vv.WriteShort(0, 10);
                vv.WriteInt(10, 20);
                vv.WriteDouble(20, 3.444);
                Assert.IsTrue(vv.ReadShort(0) == 10);
                Assert.IsTrue(vv.ReadInt(10) == 20);
                Assert.IsTrue(vv.ReadDouble(20) == 3.444);
            }
            sw.Stop();

        }
    }
}