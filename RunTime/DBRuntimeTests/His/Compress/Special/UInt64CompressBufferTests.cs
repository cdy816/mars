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
    public class UInt64CompressBufferTests
    {
        [TestMethod()]
        public void CompressTest()
        {
            UInt64CompressBuffer icb = new UInt64CompressBuffer(300) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            for (int i = 0; i < 300; i++)
            {
                icb.Append((ulong)i);
            }
            icb.Compress();

            icb.MemoryBlock.Position = 0;

            byte[] buffer = icb.MemoryBlock.Buffers[0];

            var vals = Int64CompressBuffer.Decompress(buffer);
            for (int i = 0; i < 300; i++)
            {
                Assert.AreEqual(i, vals[i]);
            }


            UInt64CompressBuffer icb2 = new UInt64CompressBuffer(300) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            for (int i = 300; i > 0; i--)
            {
                icb2.Append((ulong)i);
            }
            icb2.Compress();

            icb2.MemoryBlock.Position = 0;

            byte[] buffer2 = icb2.MemoryBlock.Buffers[0];

            var vals2 = UInt64CompressBuffer.Decompress(buffer2);
            for (int i = 0; i < 300; i++)
            {
                Assert.AreEqual((ulong)(300 - i), vals2[i]);
            }

            icb2.Reset();
            icb2.Append(ulong.MaxValue);
            icb2.Append(ulong.MinValue);
            icb2.Append(ulong.MaxValue);
            icb2.Append(ulong.MinValue);
            icb2.Compress();

            icb2.MemoryBlock.Position = 0;
            buffer2 = icb2.MemoryBlock.Buffers[0];

            vals2 = UInt64CompressBuffer.Decompress(buffer2);

            Assert.AreEqual(ulong.MaxValue, vals2[0]);
            Assert.AreEqual(ulong.MinValue, vals2[1]);
            Assert.AreEqual(ulong.MaxValue, vals2[2]);
            Assert.AreEqual(ulong.MinValue, vals2[3]);

        }
    }
}