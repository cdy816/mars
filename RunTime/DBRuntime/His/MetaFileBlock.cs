using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class MetaFileBlock : MarshalMemoryBlock
    {
        /*
            *  一个历史文件包括：文件头文件(*.dbm2)+数据文件文件(*.dbd2)
           * ****** DBM 文件头结构 *********
           * FileHead(98)+ DataRegionPointer
           * FileHead : DateTime(8)+LastUpdateDatetime(8)+MaxtTagCount(4)+file duration(4)+block duration(4)+Time tick duration(4)+Version(2)+DatabaseName(64)
           * DataRegionPointer:[Tag1 DataPointer1(8)+...+Tag1 DataPointerN(8)(DataRegionCount)]...[Tagn DataPointer1(8)+...+Tagn DataPointerN(8)(DataRegionCount)](MaxTagCount)
        */

        public bool IsDirty { get; set; } = true;

        /// <summary>
        /// 更新某个某个变量的某个时间地址
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="addr"></param>
        public void UpdateBlockPoint(int id, DateTime time, long addr)
        {
            var FileStartHour = (time.Hour / FileDuration) * FileDuration;
            int bid = ((time.Hour - FileStartHour) * 60 + time.Minute) / BlockDuration;
            int index = (id * FileDuration * 60 / BlockDuration + bid) * 8 + 98;
            if (index + 8 > this.AllocSize)
            {
                CheckAndResize((long)(this.AllocSize * 1.5));
            }
            WriteLong(index, addr);
            if(this.Position>this.AvaiableSize)
            {
                this.AvaiableSize = (int)this.Position+2;
            }
            IsDirty = true;
        }

        /// <summary>
        /// 读取某个ID的所有ID块地址
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public List<long> ReadBlockPoints(int id)
        {
            List<long> blockPoints = new List<long>();
            var count = FileDuration * 60 / BlockDuration;

            long addr = id * count * 8 + 98;

            for (int i = 0; i < count; i++)
            {
                blockPoints.Add(ReadLong(addr + i * 8));
            }
            return blockPoints;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public long ReadBlockPoint(int id, DateTime time)
        {
            var FileStartHour = (time.Hour / FileDuration) * FileDuration;
            int bid = ((time.Hour - FileStartHour) * 60 + time.Minute) / BlockDuration;
            int index = (id * FileDuration * 60 / BlockDuration + bid) * 8 + 98;
            if (this.AllocSize < index)
            {
                return 0;
            }
            else
            {
                return ReadLong(index);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="addr"></param>
        /// <param name="mDataStream"></param>
        public void CheckAndUpdateBlockPoint(int id, DateTime time, long addr, DataFileSeriserbase mDataStream)
        {
            var ad = ReadBlockPoint(id, time);
            if (ad > 0)
            {
                //如果已经存在说明之前改区域已经更新过数据，该数据区间由多个数据块组成，多个数据块之间通过指针进行连接
                //Block Header: NextBlockAddress(5)(同一个数据区间有多个数据块时，之间通过指针关联)+DataSize(4)+ValueType(5b)+CompressType(3b)
                var pad = ad;
                do
                {
                    pad = ad;
                    if (mDataStream.Length < ad + 5)
                    {
                        break;
                    }

                    var vbyts = System.Buffers.ArrayPool<byte>.Shared.Rent(8);
                    var bss = vbyts.AsSpan<byte>();
                    bss.Clear();
                    mDataStream.ReadBytes(ad, vbyts, 0, 5);
                    ad = pad + BitConverter.ToInt64(bss);
                    System.Buffers.ArrayPool<byte>.Shared.Return(vbyts);

                }
                while (ad != pad);
                var bvals = BitConverter.GetBytes(addr - pad).AsSpan<byte>(0, 5).ToArray();
                mDataStream.Write(bvals, pad);

                if (this.Position > this.AvaiableSize)
                {
                    this.AvaiableSize = (int)this.Position+2;
                }
            }
            else
            {
                UpdateBlockPoint(id, time, addr);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int FileDuration { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int BlockDuration { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public MetaFileBlock() : base()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public MetaFileBlock(int size) : base(size)
        {

        }

        /// <summary>
        /// 可用大小
        /// </summary>
        public int AvaiableSize { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        public void UpdateLastUpdateDateTime(DateTime time)
        {
            this.WriteDatetime(8, time);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public void UpdateTagCount(int count)
        {
            this.WriteInt(16, count);
            IsDirty = true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        public void UpdateDirtyDataToDisk(System.IO.Stream stream)
        {
            if (IsDirty)
            {
                this.WriteToStream(stream, 0, this.AvaiableSize);
                IsDirty = false;
            }
        }
    }
}
