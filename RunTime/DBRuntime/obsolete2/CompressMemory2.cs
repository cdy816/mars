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

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    [Obsolete]
    public class CompressMemory2: MarshalMemoryBlock
    {

        #region ... Variables  ...

        private DateTime mCurrentTime;
        private IHisEngine2 mHisTagService;
        private Dictionary<int, CompressUnitbase2> mCompressCach = new Dictionary<int, CompressUnitbase2>();
        private Dictionary<int, long> dtmp = new Dictionary<int, long>();

        private List<int> mTagIds=new List<int>();

        private bool mIsRunning=false;

        private Queue<ManualHisDataMemoryBlock> mMemoryCach = new Queue<ManualHisDataMemoryBlock>();

        /// <summary>
        /// 
        /// </summary>
        private object mLockObj = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public CompressMemory2():base()
        {
            mHisTagService = ServiceLocator.Locator.Resolve<IHisEngine2>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public CompressMemory2(long size):base(size)
        {

        }

        #endregion ...Constructor...

        #region ... Properties ...

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
            ServiceLocator.Locator.Resolve<IDataSerialize2>().ManualRequestToSeriseFile(cdata);
            data.MakeMemoryNoBusy();

            HisDataMemoryQueryService.Service.ClearManualMemoryTime(data.Id, data.Time);
            ManualHisDataMemoryBlockPool.Pool.Release(data);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceM"></param>
        public void Init(HisDataMemoryBlockCollection sourceM)
        {
            mTagIds.Clear();
            long lsize = 0;

            //var tagserver = ServiceLocator.Locator.Resolve<IHisEngine2>();
            
            //var tags = tagserver.ListAllTags().Where(e => e.Id >= Id * TagCountPerMemory && e.Id < (Id + 1) * TagCountPerMemory).OrderBy(e => e.Id);

            foreach(var vv in CompressUnitManager2.Manager.CompressUnit)
            {
                mCompressCach.Add(vv.Key, vv.Value.Clone());
            }

            //foreach(var vv in tags)
            //{
            //    var cpt = vv.CompressType;
            //    if (!mCompressCach.ContainsKey(cpt))
            //    {
            //        mCompressCach.Add(cpt, CompressUnitManager2.Manager.GetCompressQuick(cpt).Clone());
            //    }
            //}

            foreach (var vv in sourceM.TagAddress.Where(e => e.Key >= Id * TagCountPerMemory && e.Key < (Id + 1) * TagCountPerMemory))
            {
                mTagIds.Add(vv.Key);
                dtmp.Add(vv.Key, 0);
                lsize += (vv.Value.Length+24);
            }

            this.ReAlloc(HeadSize + (long)(lsize));
            this.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        public void ReInit(HisDataMemoryBlockCollection sourceM)
        {
            mTagIds.Clear();
            long lsize = 0;
            foreach (var vv in sourceM.TagAddress.Where(e => e.Key >= Id * TagCountPerMemory && e.Key < (Id + 1) * TagCountPerMemory))
            {
                mTagIds.Add(vv.Key);
                
                if (!dtmp.ContainsKey(vv.Key))
                    dtmp.Add(vv.Key, 0);

                var cpt = mHisTagService.GetHisTag(vv.Key).CompressType;
                if (!mCompressCach.ContainsKey(cpt))
                {
                    mCompressCach.Add(cpt, CompressUnitManager2.Manager.GetCompressQuick(cpt).Clone());
                }
                if (vv.Value != null)
                    lsize += vv.Value.Length;
            }

            this.Resize(HeadSize + lsize);
        }

        /// <summary>
        /// 执行压缩
        /// </summary>
        public void Compress(HisDataMemoryBlockCollection source)
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
                try
                {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    //CheckTagAddress(source);
                    long datasize = 0;
                    int headOffset = 4 + 4;
                    long Offset = headOffset + mTagIds.Count * 8;

                    this.MakeMemoryBusy();

                    long ltmp1 = sw.ElapsedMilliseconds;

                    //更新数据区域
                    foreach (var vv in mTagIds)
                    {
                        var val = source.TagAddress[vv];
                        if (val != null)
                        {
                            var size = CompressBlockMemory(val, Offset, val.QualityAddress, val.Length, vv);
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

                    long ltmp2 = sw.ElapsedMilliseconds;

                    //写入变量、数据区对应的索引
                    int count = 0;
                    foreach (var vv in dtmp)
                    {
                        this.WriteInt(headOffset + count, (int)vv.Key);
                        this.WriteInt(headOffset + count + 4, (int)vv.Value);
                        count += 8;
                    }

                    long ltmp3 = sw.ElapsedMilliseconds;

                    ServiceLocator.Locator.Resolve<IDataSerialize2>().RequestToSeriseFile(this);
                    sw.Stop();
                    LoggerService.Service.Info("CompressEnginer", Id + "压缩完成 耗时:" + sw.ElapsedMilliseconds + " ltmp1:" + ltmp1 + " ltmp2:" + (ltmp2 - ltmp1) + " ltmp3:" + (ltmp3 - ltmp2) + " CPU Id:" + ThreadHelper.GetCurrentProcessorNumber(), ConsoleColor.Blue);

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
                var size = tp.Compress(mSourceMemory, 0, this, targetPosition + 5, len) + 1;
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
            MarshalMemoryBlock block = MarshalMemoryBlockPool.Pool.Get(data.Length * 2 + 28 + 5);
            var histag = mHisTagService.GetHisTag(data.Id);
            if (histag == null)
            {
                return null;
            }
            int datasize = 0;

            var targetPosition = 28;
            block.WriteInt(0,data.Id);
            block.WriteDatetime(4, data.Time);     //时间
            block.WriteDatetime(12, data.EndTime); //结束时间
            //block.WriteInt(20, 0);                 //写入数据大小
            block.WriteInt(24, mTagIds.Count);//写入变量的个数

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
                var size = tp.Compress(data, 0, block, targetPosition + 5, data.Length) + 1;
                block.WriteInt(targetPosition, (int)size);
                datasize = (int)(targetPosition + size + 5);
                block.WriteInt(20, datasize);                 //写入数据大小
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
            //mIsDisposed = true;
            base.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
