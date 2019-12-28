using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cdy.Tag;
//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/28 16:12:39.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Tests
{
    [TestClass()]
    public class RecordMemoryTests
    {

        [TestMethod()]
        public void ReAllocTest()
        {
            RecordMemory memory = new RecordMemory(10);
            memory.ReAlloc(20);
            Assert.IsTrue(memory.Length == 20);
        }

        [TestMethod()]
        public void ResizeTest()
        {
            RecordMemory memory = new RecordMemory(10);
            memory.Write((int)10);
            memory.Resize(20);
            memory.Position = 0;
            Assert.IsTrue(memory.ReadInt() == 10);
        }

        [TestMethod()]
        public void ClearTest()
        {
            RecordMemory memory = new RecordMemory(10);
            memory.Write((int)10);
            memory.Clear();
            memory.Position = 0;
            Assert.IsTrue(memory.ReadInt() == 0);
        }

        [TestMethod()]
        public void WriteTest()
        {
            DateTime date = DateTime.Now;

            RecordMemory memory = new RecordMemory(100);
            memory.Write(byte.MaxValue);
            memory.Write((short)short.MaxValue);
            memory.Write((ushort)ushort.MaxValue);
            memory.Write(int.MaxValue);
            memory.Write(uint.MaxValue);
            memory.Write(long.MaxValue);
            memory.Write(ulong.MaxValue);

            memory.Write(0.2f);
            memory.Write(0.2);
            
            memory.Write(date);
            memory.Write("WriteTest");

            memory.Position = 0;

            Assert.IsTrue(memory.ReadByte() == byte.MaxValue);
            Assert.IsTrue(memory.ReadShort() == short.MaxValue);
            Assert.IsTrue(memory.ReadUShort() == ushort.MaxValue);
            Assert.IsTrue(memory.ReadInt() == int.MaxValue);
            Assert.IsTrue(memory.ReadUInt() == uint.MaxValue);
            Assert.IsTrue(memory.ReadLong() == long.MaxValue);
            Assert.IsTrue(memory.ReadULong() == ulong.MaxValue);
            Assert.IsTrue(memory.ReadFloat() == 0.2f);
            Assert.IsTrue(memory.ReadDouble() == 0.2);
            Assert.IsTrue(memory.ReadDateTime() == date);
            Assert.IsTrue(memory.ReadString() == "WriteTest");

        }
       
        [TestMethod()]
        public void DisposeTest()
        {
            Assert.Fail();
        }

        [TestMethod()]
        public void CopyToTest()
        {
            DateTime date = DateTime.Now;

            RecordMemory memory = new RecordMemory(100);
            memory.Write(byte.MaxValue);
            memory.Write((short)short.MaxValue);
            memory.Write((ushort)ushort.MaxValue);
            memory.Write(int.MaxValue);
            memory.Write(uint.MaxValue);
            memory.Write(long.MaxValue);
            memory.Write(ulong.MaxValue);

            memory.Write(0.2f);
            memory.Write(0.2);

            memory.Write(date);
            memory.Write("WriteTest");

            memory.Position = 0;

            RecordMemory memory2 = new RecordMemory(100);

            memory.CopyTo(memory2,0,0,memory.Length);

            Assert.IsTrue(memory2.ReadByte() == byte.MaxValue);
            Assert.IsTrue(memory2.ReadShort() == short.MaxValue);
            Assert.IsTrue(memory2.ReadUShort() == ushort.MaxValue);
            Assert.IsTrue(memory2.ReadInt() == int.MaxValue);
            Assert.IsTrue(memory2.ReadUInt() == uint.MaxValue);
            Assert.IsTrue(memory2.ReadLong() == long.MaxValue);
            Assert.IsTrue(memory2.ReadULong() == ulong.MaxValue);
            Assert.IsTrue(memory2.ReadFloat() == 0.2f);
            Assert.IsTrue(memory2.ReadDouble() == 0.2);
            Assert.IsTrue(memory2.ReadDateTime() == date);
            Assert.IsTrue(memory2.ReadString() == "WriteTest");
        }
    }
}