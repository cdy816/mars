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
    public class Int16CompressBufferTests
    {
        [TestMethod()]
        public void CompressTest()
        {
            Int16CompressBuffer icb = new Int16CompressBuffer(300) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            for (short i = 0; i < 300; i++)
            {
                icb.Append(i);
            }
            icb.Compress();

            icb.MemoryBlock.Position = 0;

            byte[] buffer = icb.MemoryBlock.Buffers[0];

            var vals = Int16CompressBuffer.Decompress(buffer);
            for(short i=0;i<300;i++)
            {
                Assert.AreEqual(i , vals[i]);
            }


            Int16CompressBuffer icb2 = new Int16CompressBuffer(300) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            for (short i = 300; i > 0; i--)
            {
                icb2.Append(i);
            }
            icb2.Compress();

            icb2.MemoryBlock.Position = 0;

            byte[] buffer2 = icb2.MemoryBlock.Buffers[0];

            var vals2 = Int16CompressBuffer.Decompress(buffer2);
            for(short i=0;i<300;i++)
            {
                Assert.AreEqual(300-i, vals2[i]);
            }


            Int16CompressBuffer icb3 = new Int16CompressBuffer(12) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            icb3.Append(short.MaxValue);
            icb3.Append(short.MinValue);
            icb3.Append(short.MaxValue);
            icb3.Append(short.MinValue);
            icb3.Append(short.MaxValue);
            icb3.Append(short.MinValue);
            icb3.Append(short.MaxValue);
            icb3.Append(short.MinValue);
            icb3.Append(short.MaxValue);
            icb3.Append(short.MinValue);
            icb3.Append(short.MaxValue);
            icb3.Append(short.MinValue);
            icb3.Compress();

            icb3.MemoryBlock.Position = 0;

            byte[] buffer3 = icb3.MemoryBlock.Buffers[0];

            var vals3 = Int16CompressBuffer.Decompress(buffer3);
            Assert.AreEqual(short.MaxValue, vals3[0]);
            Assert.AreEqual(short.MinValue, vals3[1]);
            Assert.AreEqual(short.MaxValue, vals3[2]);
            Assert.AreEqual(short.MinValue, vals3[3]);
        }
    }
}