//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/9 9:41:02.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using DBRuntime.His;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Diagnostics;
using System.Threading;
using System.Runtime.InteropServices;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class CompressMemory3: MarshalMemoryBlock
    {

        #region ... Variables  ...

        private DateTime mCurrentTime;
        private IHisEngine3 mHisTagService;
        private Dictionary<int, CompressUnitbase2> mCompressCach = new Dictionary<int, CompressUnitbase2>();
        private Dictionary<int, long> dtmp = new Dictionary<int, long>();

        private List<int> mTagIds=new List<int>();

        private bool mIsRunning=false;

        private Queue<ManualHisDataMemoryBlock> mMemoryCach = new Queue<ManualHisDataMemoryBlock>();

        /// <summary>
        /// 
        /// </summary>
        private object mLockObj = new object();

        private HisDataMemoryBlock cachblock = new HisDataMemoryBlock();

        private IntPtr mCompressedDataPointer;

        private int mCompressedDataSize = 0;

        private MarshalMemoryBlock mStaticsMemoryBlock;

        private bool mNeedInit = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public CompressMemory3():base()
        {
            mHisTagService = ServiceLocator.Locator.Resolve<IHisEngine3>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public CompressMemory3(long size):base(size)
        {

        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public MarshalMemoryBlock StaticsMemoryBlock
        {
            get
            {
                return mStaticsMemoryBlock;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public IntPtr CompressedDataPointer
        {
            get
            {
                return mCompressedDataPointer;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int CompressedDataSize
        {
            get
            {
                return mCompressedDataSize;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public int  Id { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public static int TagCountPerMemory { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int StartId { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public int HeadSize 
        {
            get
            {
                return 8 + TagCountPerMemory * 8;
            } 
        }

        ///// <summary>
        ///// 变量内存地址缓存
        ///// Tuple 每项的含义：起始地址,值地址偏移,质量地址偏移,数据大小
        ///// </summary>
        //public Dictionary<int, HisDataMemoryBlock> TagAddress
        //{
        //    get
        //    {
        //        return mTagAddress;
        //    }
        //    set
        //    {
        //        if (mTagAddress != value)
        //        {
        //            mTagAddress = value;
        //        }
        //    }
        //}

        /// <summary>
            /// 
            /// </summary>
        public DateTime CurrentTime
        {
            get
            {
                return mCurrentTime;
            }
            set
            {
                if (mCurrentTime != value)
                {
                    mCurrentTime = value;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool HasManualCompressItems
        {
            get
            {
                return mMemoryCach.Count>0;
            }
        }

        /// <summary>
        /// 是否需要执行无损Zip压缩
        /// </summary>
        public bool IsEnableCompress { get; set; } = false;


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void ManualCompress()
        {
            lock (mLockObj)
            {
                mIsRunning = true;

                LoggerService.Service.Debug("CompressMemory", this.Name+ "开始手动压缩 数量："+mMemoryCach.Count);
                Stopwatch sw = new Stopwatch();
                sw.Start();
                while (mMemoryCach.Count > 0)
                {
                    ManualHisDataMemoryBlock vpp;
                    lock (mMemoryCach)
                    {
                        vpp = mMemoryCach.Dequeue();
                    }
                    Compress(vpp);
                    //Thread.Sleep(1);
                }
                sw.Stop();
                LoggerService.Service.Debug("CompressMemory", this.Name + "结束手动压缩 耗时：" + sw.ElapsedMilliseconds);
                mIsRunning = false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public void AddManualToCompress(ManualHisDataMemoryBlock data)
        {
            lock (mMemoryCach)
                mMemoryCach.Enqueue(data);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        private void Compress(ManualHisDataMemoryBlock data)
        {
            var cdata = CompressBlockMemory(data);
            cdata.MakeMemoryBusy();
            ServiceLocator.Locator.Resolve<IDataSerialize3>().ManualRequestToSeriseFile(cdata);
            data.MakeMemoryNoBusy();

            HisDataMemoryQueryService3.Service.ClearManualMemoryTime(data.Id, data.Time);
            ManualHisDataMemoryBlockPool.Pool.Release(data);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceM"></param>
        public void Init(HisDataMemoryBlockCollection3 sourceM)
        {
            mTagIds.Clear();
            long lsize = 0;
            StartId = Id * TagCountPerMemory;

            foreach (var vv in CompressUnitManager2.Manager.CompressUnit)
            {
                mCompressCach.Add(vv.Key, vv.Value.Clone());
            }

            foreach (var vv in sourceM.TagAddress.Where(e => e.Key >= Id * TagCountPerMemory && e.Key < (Id + 1) * TagCountPerMemory))
            {
                mTagIds.Add(vv.Key);
                dtmp.Add(vv.Key, 0);
                lsize += (sourceM.ReadDataSize(vv.Key)+24);
            }

            this.ReAlloc2(HeadSize + (long)(lsize));
            this.Clear();

            if (IsEnableCompress)
                mCompressedDataPointer = Marshal.AllocHGlobal((int)this.AllocSize);

            mStaticsMemoryBlock = new MarshalMemoryBlock(TagCountPerMemory * 52, TagCountPerMemory * 52);

        }

        /// <summary>
        /// 
        /// </summary>
        private void CheckAndResize(HisDataMemoryBlockCollection3 sourceM)
        {
            //mTagIds.Clear();
            long lsize = 0;
            foreach (var vv in sourceM.TagAddress.Where(e => e.Key >= Id * TagCountPerMemory && e.Key < (Id + 1) * TagCountPerMemory))
            {
                if (!mTagIds.Contains(vv.Key))
                {
                    mTagIds.Add(vv.Key);

                    if (!dtmp.ContainsKey(vv.Key))
                        dtmp.Add(vv.Key, 0);

                    var cpt = mHisTagService.GetHisTag(vv.Key).CompressType;
                    if (!mCompressCach.ContainsKey(cpt))
                    {
                        mCompressCach.Add(cpt, CompressUnitManager2.Manager.GetCompressQuick(cpt).Clone());
                    }
                }
                if (vv.Value >= 0)
                    lsize += sourceM.ReadDataSize(vv.Key) + 24;
            }

            if (lsize > this.AllocSize)
            {
                this.ReAlloc2(HeadSize + lsize);
                this.Clear();
            }

            if (IsEnableCompress)
            {
                Marshal.FreeHGlobal(mCompressedDataPointer);
                mCompressedDataPointer = Marshal.AllocHGlobal((int)this.AllocSize);
            }
        }

        /// <summary>
        /// 执行压缩
        /// </summary>
        public void Compress(HisDataMemoryBlockCollection3 source)
        {
            lock (mLockObj)
            {
                /*
                 内存结构:Head+数据指针区域+数据区
                 Head:数据区大小(4)+变量数量(4)
                 数据区指针:[ID(4) + address(4)]
                 数据区:[data block]
                 */
                mIsRunning = true;

                CheckAndResize(source);

                try
                {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    //CheckTagAddress(source);
                    long datasize = 0;
                    int headOffset = 4 + 4;

                    int tagheadoffset = headOffset + mTagIds.Count * 8;

                    long Offset = tagheadoffset;

                    this.MakeMemoryBusy();
                    this.StaticsMemoryBlock.MakeMemoryBusy();

                    long ltmp1 = sw.ElapsedMilliseconds;

                    //更新数据区域
                    foreach (var vv in mTagIds)
                    {
                        var val = source.TagAddress[vv];
                        if (val >=0)
                        {
                            cachblock.Reset(new IntPtr(source.ReadDataBaseAddress(vv)), source.ReadDataSize(vv));
                            var size = CompressBlockMemory(cachblock, Offset, source.ReadQualityOffsetAddress(vv), source.ReadDataSize(vv), vv);
                            if (dtmp.ContainsKey(vv))
                                dtmp[vv] = Offset;
                            Offset += size;
                            datasize += size;
                        }
                        else
                        {
                            dtmp[vv] = 0;
                        }
                    }

                    //更新指针区域
                    this.WriteInt(0, (int)datasize);//写入整体数据大小
                    this.Write((int)mTagIds.Count); //写入变量个数

                    //写入变量、数据区对应的索引
                    int count = 0;
                    foreach (var vv in dtmp)
                    {
                        this.WriteInt(headOffset + count, (int)vv.Key);
                        this.WriteInt(headOffset + count + 4, (int)(vv.Value - tagheadoffset));
                        count += 8;
                    }

                    if (IsEnableCompress)
                    {
                        ZipCompress(headOffset + mTagIds.Count * 8, (int)datasize);
                    }
                    
                    ServiceLocator.Locator.Resolve<IDataSerialize3>().RequestToSeriseFile(this);
                    sw.Stop();
                    LoggerService.Service.Info("CompressEnginer", this.Name + " 压缩完成 耗时:" + sw.ElapsedMilliseconds  + " CPU Id:" + ThreadHelper.GetCurrentProcessorNumber(), ConsoleColor.Blue);

                   
                }
                catch (Exception ex)
                {
                    LoggerService.Service.Erro("CompressEnginer", ex.StackTrace + "  " + ex.Message);
                }
                mIsRunning = false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mSourceMemory"></param>
        /// <param name="addr"></param>
        /// <param name="targetPosition"></param>
        /// <param name="len"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        private long CompressBlockMemory(HisDataMemoryBlock mSourceMemory, long targetPosition,long qulityOffset, long len, int id)
        {
            //var qulityoffset = mSourceMemory.ReadInt(addr);
            // var id = mSourceMemory.ReadInt(addr + 4);

            var histag = mHisTagService.GetHisTag(id);
            
            if (histag == null) return 0;

            var qulityoffset = qulityOffset;

            var comtype = histag.CompressType;//压缩类型

            this.CheckAndResize(targetPosition + 5 + len);

            //写入压缩类型
            this.WriteByte(targetPosition + 4, (byte)comtype);

            var tp = mCompressCach[comtype];
            if (tp != null)
            {
                tp.QulityOffset = (int)qulityoffset;
                tp.TagType = histag.TagType;
                tp.RecordType = histag.Type;
                tp.StartTime = mCurrentTime;
                tp.Parameters = histag.Parameters;
                tp.Precision = histag.Precision;
                tp.TimeTick = 100;
                tp.Id = id;

                var size = tp.Compress(mSourceMemory, 0, this, targetPosition + 5, len, mStaticsMemoryBlock,(id- StartId)*52) + 1;
                this.WriteInt(targetPosition, (int)size);
                //this.Dump();
            
                return size + 5;
            }
            
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private MarshalMemoryBlock CompressBlockMemory(ManualHisDataMemoryBlock data)
        {
            MarshalMemoryBlock block = MarshalMemoryBlockPool.Pool.Get(data.Length * 2 + 28 + 5+56);
            var histag = mHisTagService.GetHisTag(data.Id);
            if (histag == null)
            {
                return null;
            }
            int datasize = 0;

            var targetPosition = 28+56;
            //前56个字节用于统计数据存放


            block.WriteInt(56,data.Id);
            block.WriteDatetime(56+4, data.Time);     //时间
            block.WriteDatetime(56+12, data.EndTime); //结束时间

            //block.WriteInt(20, 0);                 //写入数据大小
            block.WriteInt(56 + 24, mTagIds.Count);//写入变量的个数

            var qulityoffset = data.QualityAddress;
            var comtype = histag.CompressType;//压缩类型
            block.WriteByte(targetPosition + 4, (byte)comtype);

            var tp = mCompressCach[comtype];
            if (tp != null)
            {
                tp.QulityOffset = (int)qulityoffset;
                tp.TagType = histag.TagType;
                tp.RecordType = histag.Type;
                tp.StartTime = data.Time;
                tp.Parameters = histag.Parameters;
                tp.Precision = histag.Precision;
                tp.TimeTick = data.TimeUnit;
                tp.Id = data.Id;

                var size = tp.Compress(data, 0, block, targetPosition + 5, data.Length, block, 0) + 1;
                block.WriteInt(targetPosition, (int)size);

                datasize = (int)(targetPosition + size + 5);
                block.WriteInt(56 + 20, datasize);                 //写入数据大小
            }
            return block;
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Dispose()
        {
            while (mIsRunning) Thread.Sleep(1);
            mHisTagService = null;
            mTagIds.Clear();
            mTagIds = null;

            Marshal.FreeHGlobal(mCompressedDataPointer);

            //mIsDisposed = true;
            base.Dispose();
        }

      
        /// <summary>
        /// 
        /// </summary>
        /// <param name="len"></param>
        public unsafe void ZipCompress(int start,int size)
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                if (System.IO.Compression.BrotliEncoder.TryCompress(new ReadOnlySpan<byte>((void*)(this.Handles[0] + start), size), new Span<byte>((void*)(mCompressedDataPointer + 8), (int)this.AllocSize - 8), out mCompressedDataSize))
                {
                    MemoryHelper.WriteInt32((void*)mCompressedDataPointer, 0, mCompressedDataSize);
                    MemoryHelper.WriteInt32((void*)(mCompressedDataPointer), 4, size);
                    
                    //var vtmp = Marshal.AllocHGlobal(size);
                    //int decodesize = 0;
                    //System.IO.Compression.BrotliDecoder.TryDecompress(new ReadOnlySpan<byte>((void*)(mCompressedDataPointer + 8), (int)mCompressedDataSize), new Span<byte>((void*)vtmp, size), out decodesize);
                    //Marshal.FreeHGlobal(vtmp);

                    sw.Stop();
                    LoggerService.Service.Info("CompressMemory", this.Name + " ZipCompress 耗时：" + sw.ElapsedMilliseconds + " old size:" + size + " new size:" + mCompressedDataSize);
                }
                else
                {
                    sw.Stop();
                    LoggerService.Service.Warn("CompressMemory", this.Name + " ZipCompress 耗时：" + sw.ElapsedMilliseconds + " 压缩失败！");
                }
                
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("CompressMemory", ex.Message);
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
