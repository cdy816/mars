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
    public class IntCompressBufferTests
    {
        [TestMethod()]
        public void CompressTest()
        {
            IntCompressBuffer icb = new IntCompressBuffer(300) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            for (int i = 0; i < 300; i++)
            {
                icb.Append(i);
            }
            icb.Compress();

            icb.MemoryBlock.Position = 0;

            byte[] buffer = icb.MemoryBlock.Buffers[0];

            var vals = IntCompressBuffer.Decompress(buffer);
            for(int i=0;i<300;i++)
            {
                Assert.AreEqual(i , vals[i]);
            }


            IntCompressBuffer icb2 = new IntCompressBuffer(300) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            for (int i = 300; i > 0; i--)
            {
                icb2.Append(i);
            }
            icb2.Compress();

            icb2.MemoryBlock.Position = 0;

            byte[] buffer2 = icb2.MemoryBlock.Buffers[0];

            var vals2 = IntCompressBuffer.Decompress(buffer2);
            for(int i=0;i<300;i++)
            {
                Assert.AreEqual(300-i, vals2[i]);
            }


            IntCompressBuffer icb3 = new IntCompressBuffer(12) { MemoryBlock = new MemoryBlock(300 * 10), VarintMemory = new ProtoMemory(300 * 10) };
            icb3.Append(int.MaxValue);
            icb3.Append(int.MinValue);
            icb3.Append(int.MaxValue);
            icb3.Append(int.MinValue);
            icb3.Append(int.MaxValue);
            icb3.Append(int.MinValue);
            icb3.Append(int.MaxValue);
            icb3.Append(int.MinValue);
            icb3.Append(int.MaxValue);
            icb3.Append(int.MinValue);
            icb3.Append(int.MaxValue);
            icb3.Append(int.MinValue);
            icb3.Compress();

            icb3.MemoryBlock.Position = 0;

            byte[] buffer3 = icb3.MemoryBlock.Buffers[0];

            var vals3 = IntCompressBuffer.Decompress(buffer3);
            Assert.AreEqual(int.MaxValue, vals3[0]);
            Assert.AreEqual(int.MinValue, vals3[1]);
            Assert.AreEqual(int.MaxValue, vals3[2]);
            Assert.AreEqual(int.MinValue, vals3[3]);
        }
    }
}