using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cdy.Tag;
//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/28 12:33:31.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Tests
{
    [TestClass()]
    public class ProtoMemoryTests
    {
        [TestMethod()]
        public void ProtoMemoryTest()
        {
            ProtoMemory memory = new ProtoMemory(100);
            memory.WriteInt32(10);
            memory.WriteInt32(1000);
            memory.WriteInt64(10);
            memory.WriteInt64(1000);
            memory.WriteDouble(1.0);

            Assert.IsTrue(memory.ReadInt32() == 10);
            Assert.IsTrue(memory.ReadInt32() == 1000);
            Assert.IsTrue(memory.ReadInt64() == 10);
            Assert.IsTrue(memory.ReadInt64() == 1000);
            Assert.IsTrue(Math.Round(memory.ReadDouble(),2)  == 1.0);
        }

       
    }
}