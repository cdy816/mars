using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cdy.Tag;
//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/9 9:13:04.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;

namespace Cdy.Tag.Tests
{
    [TestClass()]
    public class MarshalFixedMemoryBlockTests
    {
        //[TestMethod()]
        //public void MarshalFixedMemoryBlockTest()
        //{
        //    MarshalFixedMemoryBlock memory = new MarshalFixedMemoryBlock((long)(1024 * 1024 * 1024) * 4);

        //    MarshalFixedMemoryBlock mm = new MarshalFixedMemoryBlock(1024);

        //    memory.Clear();

        //    long ltmp = (long)(1024 * 1024 * 1024) * 3;
        //    DateTime dt = DateTime.Now;

        //    memory.WriteByte(ltmp,(byte)20);
        //    memory.WriteDatetime(memory.Position, dt);
        //    memory.WriteDouble(memory.Position, 20.5);
        //    memory.WriteFloat(memory.Position, 20.5f);
        //    memory.WriteString(memory.Position, dt.ToString(),Encoding.Unicode);
        //    memory.WriteInt(memory.Position, 20);
        //    memory.WriteLong(memory.Position, 20);
        //    memory.WriteShort(memory.Position, (short)20);
        //    memory.WriteUShort(memory.Position, (ushort)20);
        //    memory.WriteUInt(memory.Position, 20);
        //    memory.WriteULong(memory.Position, 20);

        //    memory.Position = ltmp;

        //    Assert.IsTrue(memory.ReadByte()==20);
        //    Assert.IsTrue(memory.ReadDateTime() == dt);
        //    Assert.IsTrue(memory.ReadDouble() == 20.5);
        //    Assert.IsTrue(memory.ReadFloat() == 20.5);
        //    Assert.IsTrue(memory.ReadString(Encoding.Unicode) == dt.ToString());
        //    Assert.IsTrue(memory.ReadInt() == 20);
        //    Assert.IsTrue(memory.ReadLong() == 20);
        //    Assert.IsTrue(memory.ReadShort() == 20);
        //    Assert.IsTrue(memory.ReadUShort() == 20);
        //    Assert.IsTrue(memory.ReadUInt() == 20);
        //    Assert.IsTrue(memory.ReadULong() == 20);

        //    memory.CopyTo(mm, ltmp, 0, 1024);

        //    mm.Position = 0;

        //    Assert.IsTrue(mm.ReadByte() == 20);
        //    Assert.IsTrue(mm.ReadDateTime() == dt);
        //    Assert.IsTrue(mm.ReadDouble() == 20.5);
        //    Assert.IsTrue(mm.ReadFloat() == 20.5);
        //    Assert.IsTrue(mm.ReadString(Encoding.Unicode) == dt.ToString());
        //    Assert.IsTrue(mm.ReadInt() == 20);
        //    Assert.IsTrue(mm.ReadLong() == 20);
        //    Assert.IsTrue(mm.ReadShort() == 20);
        //    Assert.IsTrue(mm.ReadUShort() == 20);
        //    Assert.IsTrue(mm.ReadUInt() == 20);
        //    Assert.IsTrue(mm.ReadULong() == 20);

        //}

        [TestMethod()]
        public void TimeTest()
        {
            MarshalFixedMemoryBlock memory = new MarshalFixedMemoryBlock((long)(1024 * 1024 * 1024) * 4);

            DateTime dt = DateTime.Now;
            long ltmp = (long)(1024 * 1024 * 1024) * 1;
            memory.WriteByte(ltmp, (byte)20);
            memory.WriteDatetime(memory.Position, dt);
            memory.WriteDouble(memory.Position, 20.5);
            memory.WriteFloat(memory.Position, 20.5f);
            memory.WriteString(memory.Position, dt.ToString(), Encoding.Unicode);
            memory.WriteInt(memory.Position, 20);
            memory.WriteLong(memory.Position, 20);
            memory.WriteShort(memory.Position, (short)20);
            memory.WriteUShort(memory.Position, (ushort)20);
            memory.WriteUInt(memory.Position, 20);
            memory.WriteULong(memory.Position, 20);

            MarshalFixedMemoryBlock mm = new MarshalFixedMemoryBlock(1024);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < 1000000; i++)
            {
                mm.WriteBytesDirect(500, mm.StartMemory, (int)ltmp, 50);
                memory.WriteUShortDirect(550, 50);
                memory.WriteByte(550, 1);
            }
            sw.Stop();
            Debug.Print("10000000:" + sw.ElapsedMilliseconds);
        }

        [TestMethod()]
        public void TimeTest2()
        {
            MarshalMemoryBlock memory = new MarshalMemoryBlock((long)(1024 * 1024 * 1024) * 3);

            DateTime dt = DateTime.Now;
            long ltmp = (long)(1024 * 1024);
            memory.WriteByte(ltmp, (byte)20);
            memory.WriteDatetime(memory.Position, dt);
            memory.WriteDouble(memory.Position, 20.5);
            memory.WriteFloat(memory.Position, 20.5f);
            memory.WriteString(memory.Position, dt.ToString(), Encoding.Unicode);
            memory.WriteInt(memory.Position, 20);
            memory.WriteLong(memory.Position, 20);
            memory.WriteShort(memory.Position, (short)20);
            memory.WriteUShort(memory.Position, (ushort)20);
            memory.WriteUInt(memory.Position, 20);
            memory.WriteULong(memory.Position, 20);

            MarshalMemoryBlock mm = new MarshalMemoryBlock(1024);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < 1000000; i++)
            {
                memory.WriteBytesDirect(500, memory.StartMemory+(int)ltmp, 0, 50);
                memory.WriteUShortDirect(550, 50);
                memory.WriteByte(550, 1);
            }
            sw.Stop();
            Debug.Print("10000000:" + sw.ElapsedMilliseconds);
        }


    }
}