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
    public class Int64CompressBufferTests
    {
        [TestMethod()]
        public void CompressTest()
        {
            Int64CompressBuffer icb = new Int64CompressBuffer(300) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            for (int i = 0; i < 300; i++)
            {
                icb.Append(i);
            }
            icb.Compress();

            icb.MemoryBlock.Position = 0;

            byte[] buffer = icb.MemoryBlock.Buffers[0];

            var vals = Int64CompressBuffer.Decompress(buffer);
            for (int i = 0; i < 300; i++)
            {
                Assert.AreEqual(i, vals[i]);
            }


            Int64CompressBuffer icb2 = new Int64CompressBuffer(300) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            for (int i = 300; i > 0; i--)
            {
                icb2.Append(i);
            }
            icb2.Compress();

            icb2.MemoryBlock.Position = 0;

            byte[] buffer2 = icb2.MemoryBlock.Buffers[0];

            var vals2 = Int64CompressBuffer.Decompress(buffer2);
            for (int i = 0; i < 300; i++)
            {
                Assert.AreEqual(300 - i, vals2[i]);
            }

            icb2.Reset();
            icb2.Append(long.MaxValue);
            icb2.Append(long.MinValue);
            icb2.Append(long.MaxValue);
            icb2.Append(long.MinValue);
            icb2.Compress();

            icb2.MemoryBlock.Position = 0;
            buffer2 = icb2.MemoryBlock.Buffers[0];

            vals2 = Int64CompressBuffer.Decompress(buffer2);

            Assert.AreEqual(long.MaxValue, vals2[0]);
            Assert.AreEqual(long.MinValue, vals2[1]);
            Assert.AreEqual(long.MaxValue, vals2[2]);
            Assert.AreEqual(long.MinValue, vals2[3]);

        }
    }
}