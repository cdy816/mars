using Microsoft.VisualStudio.TestTools.UnitTesting;
using DBRuntime.His.Compress;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cdy.Tag;

namespace DBRuntime.His.Compress.Tests
{
    [TestClass()]
    public class UIntCompressBufferTests
    {
        [TestMethod()]
        public void CompressTest()
        {
            UIntCompressBuffer icb = new UIntCompressBuffer(300) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            for (uint i = 0; i < 300; i++)
            {
                icb.Append(i);
            }
            icb.Compress();

            icb.MemoryBlock.Position = 0;

            byte[] buffer = icb.MemoryBlock.Buffers[0];

            var vals = UIntCompressBuffer.Decompress(buffer);
            for(int i=0;i<300;i++)
            {
                Assert.AreEqual((uint)i , vals[i]);
            }


            UIntCompressBuffer icb2 = new UIntCompressBuffer(300) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            for (uint i = 300; i > 0; i--)
            {
                icb2.Append(i);
            }
            icb2.Compress();

            icb2.MemoryBlock.Position = 0;

            byte[] buffer2 = icb2.MemoryBlock.Buffers[0];

            var vals2 = UIntCompressBuffer.Decompress(buffer2);
            for(int i=0;i<300;i++)
            {
                Assert.AreEqual((uint)(300-i), vals2[i]);
            }


            UIntCompressBuffer icb3 = new UIntCompressBuffer(12) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            icb3.Append(uint.MaxValue);
            icb3.Append(uint.MinValue);
            icb3.Append(uint.MaxValue);
            icb3.Append(uint.MinValue);
            icb3.Append(uint.MaxValue);
            icb3.Append(uint.MinValue);
            icb3.Append(uint.MaxValue);
            icb3.Append(uint.MinValue);
            icb3.Append(uint.MaxValue);
            icb3.Append(uint.MinValue);
            icb3.Append(uint.MaxValue);
            icb3.Append(uint.MinValue);
            icb3.Compress();

            icb3.MemoryBlock.Position = 0;

            byte[] buffer3 = icb3.MemoryBlock.Buffers[0];

            var vals3 = UIntCompressBuffer.Decompress(buffer3);
            Assert.AreEqual(uint.MaxValue, vals3[0]);
            Assert.AreEqual(uint.MinValue, vals3[1]);
            Assert.AreEqual(uint.MaxValue, vals3[2]);
            Assert.AreEqual(uint.MinValue, vals3[3]);
        }
    }
}