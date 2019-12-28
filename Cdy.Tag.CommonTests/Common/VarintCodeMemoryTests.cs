using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cdy.Tag;
//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/28 15:50:27.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Tests
{
    [TestClass()]
    public class VarintCodeMemoryTests
    {

        [TestMethod()]
        public void WriteSInt32Test()
        {
            VarintCodeMemory vm = new VarintCodeMemory(100);
            vm.WriteSInt32(10);
            vm.WriteSInt32(2345);
            vm.WriteSInt32(4567);
            vm.WriteSInt32(int.MaxValue);
            vm.WriteSInt32(-1);

            vm.Position = 0;
            Assert.IsTrue(vm.ReadSInt32() == 10);
            Assert.IsTrue(vm.ReadSInt32() == 2345);
            Assert.IsTrue(vm.ReadSInt32() == 4567);
            Assert.IsTrue(vm.ReadSInt32() == int.MaxValue);
            Assert.IsTrue(vm.ReadSInt32() == -1);
        }

        [TestMethod()]
        public void WriteSInt64Test()
        {
            VarintCodeMemory vm = new VarintCodeMemory(100);
            vm.WriteSInt64(10);
            vm.WriteSInt64(2345);
            vm.WriteSInt64(4567);

            vm.Position = 0;
            Assert.IsTrue(vm.ReadSInt64() == 10);
            Assert.IsTrue(vm.ReadSInt64() == 2345);
            Assert.IsTrue(vm.ReadSInt64() == 4567);
        }

        [TestMethod()]
        public void WriteInt32Test()
        {
            VarintCodeMemory vm = new VarintCodeMemory(100);
            vm.WriteInt32(10);
            vm.WriteInt32(2345);
            vm.WriteInt32(4567);
            vm.WriteInt32(int.MaxValue);
            vm.WriteInt32(-1);

            vm.Position = 0;
            Assert.IsTrue(vm.ReadInt32() == 10);
            Assert.IsTrue(vm.ReadInt32() == 2345);
            Assert.IsTrue(vm.ReadInt32() == 4567);
            Assert.IsTrue(vm.ReadInt32() == int.MaxValue);
            Assert.IsTrue(vm.ReadInt32() == -1);
        }

        [TestMethod()]
        public void WriteInt64Test()
        {
            VarintCodeMemory vm = new VarintCodeMemory(100);
            vm.WriteInt64(10);
            vm.WriteInt64(2345);
            vm.WriteInt64(4567);

            vm.Position = 0;
            Assert.IsTrue(vm.ReadInt64() == 10);
            Assert.IsTrue(vm.ReadInt64() == 2345);
            Assert.IsTrue(vm.ReadInt64() == 4567);
        }
    }
}