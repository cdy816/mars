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
using Cdy.Tag.Driver;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class CompressMemory4: MarshalMemoryBlock
    {

        #region ... Variables  ...

        private DateTime mCurrentTime;
        private IHisEngine3 mHisTagService;
        private Dictionary<int, CompressUnitbase2> mCompressCach = new Dictionary<int, CompressUnitbase2>();
        private Dictionary<int, long> dtmp = new Dictionary<int, long>();

        //private List<int> mTagIds=new List<int>();

        private bool mIsRunning=false;

        private Queue<ManualHisDataMemoryBlock> mMemoryCach = new Queue<ManualHisDataMemoryBlock>();

        /// <summary>
        /// 
        /// </summary>
        private Queue<ManualHisDataMemoryBlockArea> mAreaMemoryCach = new Queue<ManualHisDataMemoryBlockArea>();

        /// <summary>
        /// 
        /// </summary>
        private object mLockObj = new object();

        private HisDataMemoryBlock cachblock = new HisDataMemoryBlock();

        private IntPtr mCompressedDataPointer;

        private int mCompressedDataSize = 0;

        private MarshalMemoryBlock mStaticsMemoryBlock;

        private Dictionary<string,List<int>> mAreaTags = new Dictionary<string,List<int>>();

        private int mTagCount = 0;

        //private bool mNeedInit = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public CompressMemory4():base()
        {
            mHisTagService = ServiceLocator.Locator.Resolve<IHisEngine3>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public CompressMemory4(long size):base(size)
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
                return mMemoryCach.Count>0 || mAreaMemoryCach.Count>0;
            }
        }

        /// <summary>
        /// 是否需要执行无损Zip压缩
        /// </summary>
        public bool IsEnableCompress { get; set; } = false;


        #endregion ...Properties...

        #region ... Methods    ...

        #region ManualCompress
        /// <summary>
        /// 
        /// </summary>
        public void ManualCompress()
        {
            lock (mLockObj)
            {
                mIsRunning = true;
#if DEBUG
                LoggerService.Service.Debug("CompressMemory", this.Name+ " 开始手动压缩 数量："+mMemoryCach.Count);
                Stopwatch sw = new Stopwatch();
                sw.Start();
#endif

                int icount = 0;
                while (mMemoryCach.Count > 0)
                {
                    ManualHisDataMemoryBlock vpp;
                    lock (mMemoryCach)
                    {
                        vpp = mMemoryCach.Dequeue();
                    }
                    Compress(vpp);
                    icount++;
                    //LoggerService.Service.Debug("CompressMemory", this.Name + " 手动压缩 "+icount+" 完成 剩余数据包数量：" + mMemoryCach.Count);
                    //防止大量数据包，导致内存被占用完成，及时存储并释放数据
                    if (icount > MarshalMemoryBlockPool.MaxSize)
                    {
                        if (mMemoryCach.Count > 0)
                            ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
                        break;
                    }
                    //Thread.Sleep(1);
                }

                while(mAreaMemoryCach.Count > 0)
                {
                    ManualHisDataMemoryBlockArea vpp;
                    lock (mMemoryCach)
                    {
                        vpp = mAreaMemoryCach.Dequeue();
                    }
                    icount += vpp.Area.TagCount;
                    Compress(vpp);
                    if (icount > MarshalMemoryBlockPool.MaxSize)
                    {
                        if (mMemoryCach.Count > 0)
                            ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
                        break;
                    }
                }
#if DEBUG
                sw.Stop();
                LoggerService.Service.Debug("CompressMemory", this.Name + "结束手动压缩 耗时：" + sw.ElapsedMilliseconds);
#endif
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
            {
                mMemoryCach.Enqueue(data);
            }
            if (mMemoryCach.Count > MarshalMemoryBlockPool.MaxSize / 5)
            {
                if(!mIsRunning)
                ServiceLocator.Locator.Resolve<IDataCompress3>().SubmitManualToCompress();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="area"></param>
        public void AddManualToCompress(ManualHisDataMemoryBlockArea area)
        {
            lock(mAreaMemoryCach)
            {
                mAreaMemoryCach.Enqueue(area);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        private void Compress(ManualHisDataMemoryBlock data)
        {
            try
            {
                var cdata = CompressBlockMemory(data);
                cdata.MakeMemoryBusy();
                ServiceLocator.Locator.Resolve<IDataSerialize4>().ManualRequestToSeriseFile(cdata);
                data.MakeMemoryNoBusy();

                HisDataMemoryQueryService3.Service.ClearManualMemoryTime(data.Id, data.Time);
                ManualHisDataMemoryBlockPool.Pool.Release(data);
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("CompressMemory", $"{this.Name} Compress {ex.Message} {ex.StackTrace}");
            }
        }

        private void Compress(ManualHisDataMemoryBlockArea data)
        {
            try
            {
                long timeaddr = 0;
                foreach (var vv in data.ListMemorys())
                {
                    var cdata = CompressAreaBlockMemory(vv, ref timeaddr);
                    cdata.MakeMemoryBusy();
                    ServiceLocator.Locator.Resolve<IDataSerialize4>().ManualRequestToSeriseFile(cdata);
                    vv.MakeMemoryNoBusy();

                    HisDataMemoryQueryService3.Service.ClearManualMemoryTime(vv.Id, vv.Time);

                    ManualHisDataMemoryBlockPool.Pool.Release(vv);
                }
                ManualHisDataMemoryBlockAreaPool.Instance.Return(data);
            }
            catch (Exception ex)
            {
                LoggerService.Service.Warn("CompressMemory", $"{this.Name} Compress {ex.Message} {ex.StackTrace}");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private MarshalMemoryBlock CompressAreaBlockMemory(ManualHisDataMemoryBlock data, ref long timeaddr)
        {
            //Block Header:  NextBlockAddress(5)(同一个数据区间有多个数据块时，之间通过指针关联)+DataSize(4)+ValueType(5b)+CompressType(3b)
            MarshalMemoryBlock block = MarshalMemoryBlockPool.Pool.Get(data.Length * 2 + 28 + 5 + 56 + 6);
            var histag = mHisTagService.GetHisTag(data.Id);
            if (histag == null)
            {
                return null;
            }
            int datasize = 0;

            var targetPosition = 36 + 56;
            //前56个字节用于统计数据存放
            //Head(36): ID(4) + Time(8)+RealStartTime(8)+EndTime(8) + DataSize(4)+ TagCount(4)
            block.WriteInt(56, data.Id);
            block.WriteDatetime(56 + 4, data.Time);     //时间
            block.WriteDatetime(56 + 12, data.RealTime);//实际开始时间
            block.WriteDatetime(56 + 20, data.EndTime); //结束时间

            //block.WriteInt(28, 0);            //写入数据大小
            block.WriteInt(56 + 32, mTagCount);//写入变量的个数

            var qulityoffset = data.QualityAddress;
            var comtype = histag.CompressType;//压缩类型

            //block head:next pointer(5)+compress(1)+datasize(4)
            //将下一个块的指针清零
            block.WriteInt(targetPosition, 0);
            block.WriteByte(targetPosition + 4, 0);
            //写入变量类型+压缩类型
            byte bsval = (byte)(comtype | (byte)(histag.TagType + 1) << 3);
            block.WriteByte(targetPosition + 9, (byte)bsval);

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
                //tp.TimeTick = 1;//
                tp.Id = data.Id;

                var size = tp.CompressByArea(data, 0, block, targetPosition + 10, data.UsedSize, block, 0, ref timeaddr) + 1;//将变量类型+压缩类型一个字节加上
                block.WriteInt(targetPosition + 5, (int)size);

                datasize = (int)(size + 10);
                timeaddr += datasize;
                //写入数据大小
                block.WriteInt(56 + 28, (int)datasize);
            }
            return block;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="ctype"></param>
        /// <param name="rtype"></param>
        /// <param name="ttype"></param>
        /// <param name="Precision"></param>
        /// <param name="Parameters"></param>
        /// <returns></returns>
        public MarshalMemoryBlock CompressBlockMemory(ManualHisDataMemoryBlock data, int ctype, RecordType rtype, TagType ttype, int Precision, Dictionary<string, double> Parameters)
        {
            //Block Header:  NextBlockAddress(5)(同一个数据区间有多个数据块时，之间通过指针关联)+DataSize(4)+ValueType(5b)+CompressType(3b)
            MarshalMemoryBlock block = MarshalMemoryBlockPool.Pool.Get(data.Length * 2 + 28 + 5 + 56 + 6);

            int datasize = 0;

            var targetPosition = 36 + 56;
            //前56个字节用于统计数据存放
            //Head(36): ID(4) + Time(8)+RealStartTime(8)+EndTime(8) + DataSize(4)+ TagCount(4)
            block.WriteInt(56, data.Id);
            block.WriteDatetime(56 + 4, data.Time);     //时间
            block.WriteDatetime(56 + 12, data.RealTime);//实际开始时间
            block.WriteDatetime(56 + 20, data.EndTime); //结束时间

            //block.WriteInt(28, 0);                 //写入数据大小
            block.WriteInt(56 + 32, mTagCount);//写入变量的个数

            var qulityoffset = data.QualityAddress;
            var comtype = ctype;//压缩类型

            //block head:next pointer(5)+compress(1)+datasize(4)
            //将下一个块的指针清零
            block.WriteInt(targetPosition, 0);
            block.WriteByte(targetPosition + 4, 0);
            //写入变量类型+压缩类型
            byte bsval = (byte)(comtype | (byte)(ttype + 1) << 3);
            block.WriteByte(targetPosition + 9, (byte)bsval);

            var tp = mCompressCach[comtype];
            if (tp != null)
            {
                tp.QulityOffset = (int)qulityoffset;
                tp.TagType = ttype;
                tp.RecordType = rtype;
                tp.StartTime = data.Time;
                tp.Parameters = Parameters;
                tp.Precision = Precision;
                tp.TimeTick = data.TimeUnit;
                tp.Id = data.Id;

                var size = tp.Compress(data, 0, block, targetPosition + 10, data.Length, block, 0) + 1;//将变量类型+压缩类型一个字节加上
                block.WriteInt(targetPosition + 5, (int)size);

                datasize = (int)(size + 10);

                //写入数据大小
                block.WriteInt(56 + 28, (int)datasize);
            }
            return block;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private MarshalMemoryBlock CompressBlockMemory(ManualHisDataMemoryBlock data)
        {
            //Block Header:  NextBlockAddress(5)(同一个数据区间有多个数据块时，之间通过指针关联)+DataSize(4)+ValueType(5b)+CompressType(3b)
            MarshalMemoryBlock block = MarshalMemoryBlockPool.Pool.Get(data.Length * 2 + 28 + 5 + 56 + 6);
            var histag = mHisTagService.GetHisTag(data.Id);
            if (histag == null)
            {
                return null;
            }
            int datasize = 0;

            var targetPosition = 36 + 56;
            //前56个字节用于统计数据存放
            //Head(36): ID(4) + Time(8)+RealStartTime(8)+EndTime(8) + DataSize(4)+ TagCount(4)
            block.WriteInt(56, data.Id);
            block.WriteDatetime(56 + 4, data.Time);     //时间
            block.WriteDatetime(56 + 12, data.RealTime);//实际开始时间
            block.WriteDatetime(56 + 20, data.EndTime); //结束时间

            //block.WriteInt(28, 0);                 //写入数据大小
            block.WriteInt(56 + 32, mTagCount);//写入变量的个数

            var qulityoffset = data.QualityAddress;
            var comtype = histag.CompressType;//压缩类型

            //block head:next pointer(5)+compress(1)+datasize(4)
            //将下一个块的指针清零
            block.WriteInt(targetPosition, 0);
            block.WriteByte(targetPosition + 4, 0);
            //写入变量类型+压缩类型
            byte bsval = (byte)(comtype | (byte)(histag.TagType + 1) << 3);
            block.WriteByte(targetPosition + 9, (byte)bsval);

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
                //tp.TimeTick = 1;//
                tp.Id = data.Id;

                var size = tp.Compress(data, 0, block, targetPosition + 10, data.UsedSize, block, 0) + 1;//将变量类型+压缩类型一个字节加上
                block.WriteInt(targetPosition + 5, (int)size);

                datasize = (int)(size + 10);

                //写入数据大小
                block.WriteInt(56 + 28, (int)datasize);
            }
            return block;
        }

        #endregion
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceM"></param>
        public void Init(HisDataMemoryBlockCollection3 sourceM)
        {
            lock (mLockObj)
            {
                //mTagIds.Clear();
                mAreaTags.Clear();
                dtmp.Clear();
                mCompressCach.Clear();
                mTagCount = 0;

                long lsize = 0;
                StartId = Id * TagCountPerMemory;

                foreach (var vv in CompressUnitManager2.Manager.CompressUnit)
                {
                    mCompressCach.Add(vv.Key, vv.Value.Clone());
                }
                var service = ServiceLocator.Locator.Resolve<IRealTagProduct>();

                foreach (var vv in sourceM.TagAddress.Where(e => e.Key >= Id * TagCountPerMemory && e.Key < (Id + 1) * TagCountPerMemory))
                {
                    //mTagIds.Add(vv.Key);

                    var vtag = service.GetTagById(vv.Key);
                    string area = vtag.Area;
                    if (string.IsNullOrEmpty(vtag.Area))
                    {
                        area = "";
                    }
                    if (mAreaTags.ContainsKey(area))
                    {
                        mAreaTags[area].Add(vtag.Id);
                    }
                    else
                    {
                        mAreaTags.Add(area, new List<int> { vtag.Id });
                    }

                    dtmp.Add(vv.Key, 0);
                    lsize += (sourceM.ReadDataSizeByIndex(vv.Value) + 24);
                    mTagCount++;
                }

                this.ReAlloc2(HeadSize + (long)(lsize));
                this.Clear();

                if (mCompressedDataPointer != IntPtr.Zero)
                {
                    Marshal.FreeHGlobal(mCompressedDataPointer);
                }

                if (IsEnableCompress)
                    mCompressedDataPointer = Marshal.AllocHGlobal((int)this.AllocSize);

                if (mStaticsMemoryBlock != null)
                {
                    mStaticsMemoryBlock.Dispose();
                }

                mStaticsMemoryBlock = new MarshalMemoryBlock(TagCountPerMemory * 52, TagCountPerMemory * 52);

            
            }

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
                //if (!mTagIds.Contains(vv.Key))
                //{
                //    mTagIds.Add(vv.Key);

                //    if (!dtmp.ContainsKey(vv.Key))
                //        dtmp.Add(vv.Key, 0);
                //}

                var cpt = mHisTagService.GetHisTag(vv.Key).CompressType;
                if (!mCompressCach.ContainsKey(cpt))
                {
                    mCompressCach.Add(cpt, CompressUnitManager2.Manager.GetCompressQuick(cpt).Clone());
                }

                if (vv.Value >= 0)
                    lsize += sourceM.ReadDataSizeByIndex(vv.Value) + 24+6;
            }

            if (lsize > this.AllocSize)
            {
                this.ReAlloc2(HeadSize + lsize);
                this.Clear();
            }

            //if (IsEnableCompress)
            //{
            //    Marshal.FreeHGlobal(mCompressedDataPointer);
            //    mCompressedDataPointer = Marshal.AllocHGlobal((int)this.AllocSize);
            //}
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

                //如果还没有初始化
                if (mStaticsMemoryBlock == null) return;

                CheckAndResize(source);

                try
                {
                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    //CheckTagAddress(source);
                    long datasize = 0;
                    int headOffset = 4 + 4;

                    int tagheadoffset = headOffset + mTagCount * 8;

                    long Offset = tagheadoffset;

                    this.MakeMemoryBusy();
                    this.StaticsMemoryBlock.MakeMemoryBusy();

                    long ltmp1 = sw.ElapsedMilliseconds;

                   
                    foreach(var vva in mAreaTags)
                    {
                        if(vva.Key=="")
                        {
                            //更新数据区域
                            foreach (var vv in vva.Value)
                            {
                                var val = source.TagAddress[vv];
                                if (val >= 0)
                                {
                                    cachblock.Reset(new IntPtr(source.ReadDataBaseAddressByIndex(val)), source.ReadDataSizeByIndex(val));
                                    var size = CompressBlockMemory(cachblock, Offset, source.ReadQualityOffsetAddressByIndex(val), source.ReadDataSizeByIndex(val), vv);
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
                        }
                        else
                        {
                            long timeaddr = 0;
                            //更新数据区域
                            foreach (var vv in vva.Value)
                            {
                                var val = source.TagAddress[vv];
                                if (val >= 0)
                                {
                                    cachblock.Reset(new IntPtr(source.ReadDataBaseAddressByIndex(val)), source.ReadDataSizeByIndex(val));
                                    var size = CompressBlockAreaMemory(cachblock, Offset, source.ReadQualityOffsetAddressByIndex(val), source.ReadDataSizeByIndex(val), vv,ref timeaddr);
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
                        }
                    }

                    

                    //更新指针区域
                    this.WriteInt(0, (int)datasize);//写入整体数据大小
                    this.Write((int)mTagCount); //写入变量个数

                    //写入变量、数据区对应的索引
                    int count = 0;
                    foreach (var vv in dtmp)
                    {
                        this.WriteInt(headOffset + count, (int)vv.Key);
                        this.WriteInt(headOffset + count + 4, (int)(vv.Value - tagheadoffset));
                        count += 8;
                    }
                    
                    ServiceLocator.Locator.Resolve<IDataSerialize4>().RequestToSeriseFile(this);
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
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startime"></param>
        /// <param name="datas"></param>
        /// <returns></returns>
        public MarshalMemoryBlock CompressData<T>(int id, DateTime startime, HisQueryResult<T> datas)
        {
            var histag = mHisTagService.GetHisTag(id);
            if (histag == null)
            {
                return null;
            }

            if (datas.Count == 0)
            {
                return null;
            }
            else
            {
                MarshalMemoryBlock block = MarshalMemoryBlockPool.Pool.Get(datas.Count * 264);

                HisDataMemoryBlock mmb = new HisDataMemoryBlock(datas.Count * 264) { TimeLen = 4 };

                int valueaddress = datas.Count * 4;
                int qualityaddress = 0;
                switch (histag.TagType)
                {
                    case TagType.Bool:
                        qualityaddress = valueaddress + datas.Count;
                        break;
                    case TagType.Byte:
                        qualityaddress = valueaddress + datas.Count;
                        break;
                    case TagType.Short:
                    case TagType.UShort:
                        qualityaddress = valueaddress + datas.Count * 2;
                        break;
                    case TagType.Int:
                    case TagType.UInt:
                    case TagType.Float:
                        qualityaddress = valueaddress + datas.Count * 4;
                        break;
                    case TagType.Long:
                    case TagType.ULong:
                    case TagType.DateTime:
                    case TagType.Double:
                        qualityaddress = valueaddress + datas.Count * 8;
                        break;
                    case TagType.UIntPoint:
                    case TagType.IntPoint:
                        qualityaddress = valueaddress + datas.Count * 8;
                        break;
                    case TagType.UIntPoint3:

                    case TagType.IntPoint3:
                        qualityaddress = valueaddress + datas.Count * 12;
                        break;
                    case TagType.ULongPoint:
                    case TagType.LongPoint:
                        qualityaddress = valueaddress + datas.Count * 16;
                        break;
                    case TagType.ULongPoint3:
                    case TagType.LongPoint3:
                        qualityaddress = valueaddress + datas.Count * 24;
                        break;
                    case TagType.String:
                        qualityaddress = valueaddress + datas.Count * 256;
                        break;
                }


                for (int i = 0; i < datas.Count; i++)
                {
                    var val = datas.GetValue(i, out DateTime time, out byte quality);
                    if (time == DateTime.MinValue) continue;

                    var vtime = (int)((time - startime).TotalMilliseconds / 1);
                    //写入时间戳
                    mmb.WriteInt(i * 4, vtime);
                    switch (histag.TagType)
                    {
                        case TagType.Bool:
                            mmb.WriteByteDirect(valueaddress + i * histag.SizeOfValue, Convert.ToByte(Convert.ToBoolean(val)));
                            break;
                        case TagType.Byte:
                            mmb.WriteByteDirect(valueaddress + i * histag.SizeOfValue, Convert.ToByte(val));
                            break;
                        case TagType.Short:
                            mmb.WriteShortDirect(valueaddress + i * histag.SizeOfValue, Convert.ToInt16(val));
                            break;
                        case TagType.UShort:
                            mmb.WriteUShortDirect(valueaddress + i * histag.SizeOfValue, Convert.ToUInt16(val));
                            break;
                        case TagType.Int:
                            mmb.WriteIntDirect(valueaddress + i * histag.SizeOfValue, Convert.ToInt32(val));
                            break;
                        case TagType.UInt:
                            mmb.WriteUIntDirect(valueaddress + i * histag.SizeOfValue, Convert.ToUInt32(val));
                            break;
                        case TagType.Long:
                            mmb.WriteLongDirect(valueaddress + i * histag.SizeOfValue, Convert.ToInt64(val));
                            break;
                        case TagType.ULong:
                            mmb.WriteULongDirect(valueaddress + i * histag.SizeOfValue, Convert.ToUInt64(val));
                            break;
                        case TagType.Float:
                            mmb.WriteFloatDirect(valueaddress + i * histag.SizeOfValue, Convert.ToSingle(val));
                            break;
                        case TagType.Double:
                            mmb.WriteDoubleDirect(valueaddress + i * histag.SizeOfValue, Convert.ToDouble(val));
                            break;
                        case TagType.String:
                            mmb.WriteStringDirect(valueaddress + i * histag.SizeOfValue, Convert.ToString(val), Encoding.Unicode);
                            break;
                        case TagType.DateTime:
                            mmb.WriteDatetime(valueaddress + i * histag.SizeOfValue, Convert.ToDateTime(val));
                            break;
                        case TagType.UIntPoint:
                            UIntPointData data = (UIntPointData)(object)val;
                            mmb.WriteUIntDirect(valueaddress + i * histag.SizeOfValue, data.X);
                            mmb.WriteUIntDirect(valueaddress + i * histag.SizeOfValue + 4, data.Y);
                            break;
                        case TagType.IntPoint:
                            IntPointData idata = (IntPointData)(object)val;
                            mmb.WriteIntDirect(valueaddress + i * histag.SizeOfValue, idata.X);
                            mmb.WriteIntDirect(valueaddress + i * histag.SizeOfValue + 4, idata.Y);
                            break;
                        case TagType.UIntPoint3:
                            UIntPoint3Data udata3 = (UIntPoint3Data)(object)val;
                            mmb.WriteUIntDirect(valueaddress + i * histag.SizeOfValue, udata3.X);
                            mmb.WriteUIntDirect(valueaddress + i * histag.SizeOfValue + 4, udata3.Y);
                            mmb.WriteUIntDirect(valueaddress + i * histag.SizeOfValue + 8, udata3.Z);
                            break;
                        case TagType.IntPoint3:
                            IntPoint3Data idata3 = (IntPoint3Data)(object)val;
                            mmb.WriteIntDirect(valueaddress + i * histag.SizeOfValue, idata3.X);
                            mmb.WriteIntDirect(valueaddress + i * histag.SizeOfValue + 4, idata3.Y);
                            mmb.WriteIntDirect(valueaddress + i * histag.SizeOfValue + 8, idata3.Z);
                            break;

                        case TagType.ULongPoint:
                            ULongPointData udata = (ULongPointData)(object)val;
                            mmb.WriteULongDirect(valueaddress + i * histag.SizeOfValue, udata.X);
                            mmb.WriteULongDirect(valueaddress + i * histag.SizeOfValue + 8, udata.Y);
                            break;
                        case TagType.LongPoint:
                            LongPointData lidata = (LongPointData)(object)val;
                            mmb.WriteLongDirect(valueaddress + i * histag.SizeOfValue, lidata.X);
                            mmb.WriteLongDirect(valueaddress + i * histag.SizeOfValue + 8, lidata.Y);
                            break;
                        case TagType.ULongPoint3:
                            ULongPoint3Data ludata3 = (ULongPoint3Data)(object)val;
                            mmb.WriteULongDirect(valueaddress + i * histag.SizeOfValue, ludata3.X);
                            mmb.WriteULongDirect(valueaddress + i * histag.SizeOfValue + 16, ludata3.Y);
                            mmb.WriteULongDirect(valueaddress + i * histag.SizeOfValue + 24, ludata3.Z);
                            break;
                        case TagType.LongPoint3:
                            LongPoint3Data lidata3 = (LongPoint3Data)(object)val;
                            mmb.WriteLongDirect(valueaddress + i * histag.SizeOfValue, lidata3.X);
                            mmb.WriteLongDirect(valueaddress + i * histag.SizeOfValue + 16, lidata3.Y);
                            mmb.WriteLongDirect(valueaddress + i * histag.SizeOfValue + 24, lidata3.Z);
                            break;
                    }
                    mmb.WriteByte(qualityaddress + i, quality);
                }

                var comtype = histag.CompressType;//压缩类型
                                                  //将Byte的高5位用作表示变量类型（最多支持31种数据类型）(将变量类型值值+1，使得记录的值从1开始，用于和之前的区分，做兼容)，低三位表示压缩类型（最多支持8种压缩）
                byte bsval = (byte)(comtype | (byte)(histag.TagType + 1) << 3);
                block.WriteByte(9, (byte)bsval);
                var tp = mCompressCach[comtype];
                if (tp != null)
                {
                    tp.QulityOffset = qualityaddress;
                    tp.TagType = histag.TagType;
                    tp.RecordType = histag.Type;
                    tp.StartTime = startime;
                    tp.Parameters = histag.Parameters;
                    tp.Precision = histag.Precision;
                    tp.TimeTick = 1;
                    tp.Id = id;

                    var size = tp.CompressWithNoStatistic(mmb, 0, block, 10, mmb.Position) + 1;//将变量类型+压缩类型一个字节加上

                    var vpos = block.Position;

                    block.WriteInt(5, (int)size);

                    block.Position = vpos;
                }
                mmb.Dispose();

                return block;
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startime"></param>
        /// <param name="datas"></param>
        /// <returns></returns>
        public MarshalMemoryBlock CompressData<T>(int id, DateTime startime, SortedDictionary<DateTime, T> datas, SortedDictionary<DateTime, byte> qualitys)
        {
            var histag = mHisTagService.GetHisTag(id);
            if (histag == null)
            {
                return null;
            }
            MarshalMemoryBlock block = MarshalMemoryBlockPool.Pool.Get(datas.Count * 264);

            MarshalFixedMemoryBlock mmb = new MarshalFixedMemoryBlock(datas.Count * 264);

            int valueaddress = datas.Count * 4;
            int qualityaddress = 0;
            switch (histag.TagType)
            {
                case TagType.Bool:
                    qualityaddress = valueaddress + datas.Count;
                    break;
                case TagType.Byte:
                    qualityaddress = valueaddress + datas.Count;
                    break;
                case TagType.Short:
                case TagType.UShort:
                    qualityaddress = valueaddress + datas.Count * 2;
                    break;
                case TagType.Int:
                case TagType.UInt:
                case TagType.Float:
                    qualityaddress = valueaddress + datas.Count * 4;
                    break;
                case TagType.Long:
                case TagType.ULong:
                case TagType.DateTime:
                case TagType.Double:
                    qualityaddress = valueaddress + datas.Count * 8;
                    break;
                case TagType.UIntPoint:
                case TagType.IntPoint:
                    qualityaddress = valueaddress + datas.Count * 8;
                    break;
                case TagType.UIntPoint3:

                case TagType.IntPoint3:
                    qualityaddress = valueaddress + datas.Count * 12;
                    break;
                case TagType.ULongPoint:
                case TagType.LongPoint:
                    qualityaddress = valueaddress + datas.Count * 16;
                    break;
                case TagType.ULongPoint3:
                case TagType.LongPoint3:
                    qualityaddress = valueaddress + datas.Count * 24;
                    break;
                case TagType.String:
                    qualityaddress = valueaddress + datas.Count * 256;
                    break;
            }
            int icount = 0;
            foreach (var dt in datas)
            {
                var vtime = (int)((dt.Key - startime).TotalMilliseconds / 1);
                //写入时间戳
                mmb.WriteInt(icount * 4, vtime);
                switch (histag.TagType)
                {
                    case TagType.Bool:
                        mmb.WriteByteDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToByte(Convert.ToBoolean(dt.Value)));
                        break;
                    case TagType.Byte:
                        mmb.WriteByteDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToByte(dt.Value));
                        break;
                    case TagType.Short:
                        mmb.WriteShortDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToInt16(dt.Value));
                        break;
                    case TagType.UShort:
                        mmb.WriteUShortDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToUInt16(dt.Value));
                        break;
                    case TagType.Int:
                        mmb.WriteIntDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToInt32(dt.Value));
                        break;
                    case TagType.UInt:
                        mmb.WriteUIntDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToUInt32(dt.Value));
                        break;
                    case TagType.Long:
                        mmb.WriteLongDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToInt64(dt.Value));
                        break;
                    case TagType.ULong:
                        mmb.WriteULongDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToUInt64(dt.Value));
                        break;
                    case TagType.Float:
                        mmb.WriteFloatDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToSingle(dt.Value));
                        break;
                    case TagType.Double:
                        mmb.WriteDoubleDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToDouble(dt.Value));
                        break;
                    case TagType.String:
                        mmb.WriteStringDirect(valueaddress + icount * histag.SizeOfValue, Convert.ToString(dt.Value), Encoding.Unicode);
                        break;
                    case TagType.DateTime:
                        mmb.WriteDatetime(valueaddress + icount * histag.SizeOfValue, Convert.ToDateTime(dt.Value));
                        break;
                    case TagType.UIntPoint:
                        UIntPointData data = (UIntPointData)(object)dt.Value;
                        mmb.WriteUIntDirect(valueaddress + icount * histag.SizeOfValue, data.X);
                        mmb.WriteUIntDirect(valueaddress + icount * histag.SizeOfValue + 4, data.Y);
                        break;
                    case TagType.IntPoint:
                        IntPointData idata = (IntPointData)(object)dt.Value;
                        mmb.WriteIntDirect(valueaddress + icount * histag.SizeOfValue, idata.X);
                        mmb.WriteIntDirect(valueaddress + icount * histag.SizeOfValue + 4, idata.Y);
                        break;
                    case TagType.UIntPoint3:
                        UIntPoint3Data udata3 = (UIntPoint3Data)(object)dt.Value;
                        mmb.WriteUIntDirect(valueaddress + icount * histag.SizeOfValue, udata3.X);
                        mmb.WriteUIntDirect(valueaddress + icount * histag.SizeOfValue + 4, udata3.Y);
                        mmb.WriteUIntDirect(valueaddress + icount * histag.SizeOfValue + 8, udata3.Z);
                        break;
                    case TagType.IntPoint3:
                        IntPoint3Data idata3 = (IntPoint3Data)(object)dt.Value;
                        mmb.WriteIntDirect(valueaddress + icount * histag.SizeOfValue, idata3.X);
                        mmb.WriteIntDirect(valueaddress + icount * histag.SizeOfValue + 4, idata3.Y);
                        mmb.WriteIntDirect(valueaddress + icount * histag.SizeOfValue + 8, idata3.Z);
                        break;

                    case TagType.ULongPoint:
                        ULongPointData udata = (ULongPointData)(object)dt.Value;
                        mmb.WriteULongDirect(valueaddress + icount * histag.SizeOfValue, udata.X);
                        mmb.WriteULongDirect(valueaddress + icount * histag.SizeOfValue + 8, udata.Y);
                        break;
                    case TagType.LongPoint:
                        LongPointData lidata = (LongPointData)(object)dt.Value;
                        mmb.WriteLongDirect(valueaddress + icount * histag.SizeOfValue, lidata.X);
                        mmb.WriteLongDirect(valueaddress + icount * histag.SizeOfValue + 8, lidata.Y);
                        break;
                    case TagType.ULongPoint3:
                        ULongPoint3Data ludata3 = (ULongPoint3Data)(object)dt.Value;
                        mmb.WriteULongDirect(valueaddress + icount * histag.SizeOfValue, ludata3.X);
                        mmb.WriteULongDirect(valueaddress + icount * histag.SizeOfValue + 16, ludata3.Y);
                        mmb.WriteULongDirect(valueaddress + icount * histag.SizeOfValue + 24, ludata3.Z);
                        break;
                    case TagType.LongPoint3:
                        LongPoint3Data lidata3 = (LongPoint3Data)(object)dt.Value;
                        mmb.WriteLongDirect(valueaddress + icount * histag.SizeOfValue, lidata3.X);
                        mmb.WriteLongDirect(valueaddress + icount * histag.SizeOfValue + 16, lidata3.Y);
                        mmb.WriteLongDirect(valueaddress + icount * histag.SizeOfValue + 24, lidata3.Z);
                        break;
                }
                mmb.WriteByte(qualityaddress + icount, qualitys[dt.Key]);

                icount++;
            }

            var comtype = histag.CompressType;//压缩类型
             //将Byte的高5位用作表示变量类型（最多支持31种数据类型）(将变量类型值值+1，使得记录的值从1开始，用于和之前的区分，做兼容)，低三位表示压缩类型（最多支持8种压缩）
            byte bsval = (byte)(comtype | (byte)(histag.TagType + 1) << 3);
            block.WriteByte(9, (byte)bsval);
            var tp = mCompressCach[comtype];
            if (tp != null)
            {
                tp.QulityOffset = qualityaddress;
                tp.TagType = histag.TagType;
                tp.RecordType = histag.Type;
                tp.StartTime = startime;
                tp.Parameters = histag.Parameters;
                tp.Precision = histag.Precision;
                tp.TimeTick = 1;
                tp.Id = id;

                var size = tp.CompressWithNoStatistic(mmb, 0, block, 10, mmb.AllocSize) + 1;//将变量类型+压缩类型一个字节加上
                var vpos = block.Position;
                block.WriteInt(5, (int)size);
                block.Position = vpos;
            }
            mmb.Dispose();

            return block;

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
            
            //Block Header:  NextBlockAddress(5)(同一个数据区间有多个数据块时，之间通过指针关联)+DataSize(4)+ValueType(5b)+CompressType(3b)

            var histag = mHisTagService.GetHisTag(id);
            
            if (histag == null) return 0;

            var qulityoffset = qulityOffset;

            var comtype = histag.CompressType;//压缩类型

            this.CheckAndResize(targetPosition + 10 + len);

            //将Byte的高5位用作表示变量类型（最多支持31种数据类型）(将变量类型值值+1，使得记录的值从1开始，用于和之前的区分，做兼容)，低三位表示压缩类型（最多支持8种压缩）
            byte bsval = (byte)(comtype | (byte)(histag.TagType+1) << 3);

            ////写入压缩类型
            //this.WriteByte(targetPosition + 4, (byte)comtype);

            //将下一个块的指针清零
            this.WriteInt(targetPosition, 0);
            this.WriteByte(targetPosition + 4, 0);
            //写入变量类型+压缩类型
            this.WriteByte(targetPosition + 9, (byte)bsval);

            var tp = mCompressCach[comtype];
            if (tp != null)
            {
                tp.QulityOffset = (int)qulityoffset;
                tp.TagType = histag.TagType;
                tp.RecordType = histag.Type;
                tp.StartTime = mCurrentTime;
                tp.Parameters = histag.Parameters;
                tp.Precision = histag.Precision;
                tp.TimeTick = HisEnginer3.MemoryTimeTick;
                tp.Id = id;

                var size = tp.Compress(mSourceMemory, 0, this, targetPosition + 10, len, mStaticsMemoryBlock,(id- StartId)*52)+1; //将变量类型+压缩类型一个字节加上
                //写入大小
                this.WriteInt(targetPosition+5, (int)size);
                //this.Dump();
            
                return size + 9;
            }
            
            return 0;
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
        private long CompressBlockAreaMemory(HisDataMemoryBlock mSourceMemory, long targetPosition, long qulityOffset, long len, int id, ref long timeaddr)
        {
            //var qulityoffset = mSourceMemory.ReadInt(addr);
            // var id = mSourceMemory.ReadInt(addr + 4);

            //Block Header:  NextBlockAddress(5)(同一个数据区间有多个数据块时，之间通过指针关联)+DataSize(4)+ValueType(5b)+CompressType(3b)

            var histag = mHisTagService.GetHisTag(id);

            if (histag == null) return 0;

            var qulityoffset = qulityOffset;

            var comtype = histag.CompressType;//压缩类型

            this.CheckAndResize(targetPosition + 10 + len);

            //将Byte的高5位用作表示变量类型（最多支持31种数据类型）(将变量类型值值+1，使得记录的值从1开始，用于和之前的区分，做兼容)，低三位表示压缩类型（最多支持8种压缩）
            byte bsval = (byte)(comtype | (byte)(histag.TagType + 1) << 3);

            ////写入压缩类型
            //this.WriteByte(targetPosition + 4, (byte)comtype);

            //将下一个块的指针清零
            this.WriteInt(targetPosition, 0);
            this.WriteByte(targetPosition + 4, 0);
            //写入变量类型+压缩类型
            this.WriteByte(targetPosition + 9, (byte)bsval);

            var tp = mCompressCach[comtype];
            if (tp != null)
            {
                tp.QulityOffset = (int)qulityoffset;
                tp.TagType = histag.TagType;
                tp.RecordType = histag.Type;
                tp.StartTime = mCurrentTime;
                tp.Parameters = histag.Parameters;
                tp.Precision = histag.Precision;
                tp.TimeTick = HisEnginer3.MemoryTimeTick;
                tp.Id = id;

                var size = tp.CompressByArea(mSourceMemory, 0, this, targetPosition + 10, len, mStaticsMemoryBlock, (id - StartId) * 52,ref timeaddr) + 1; //将变量类型+压缩类型一个字节加上
                                                                                                                                                           //写入大小
                this.WriteInt(targetPosition + 5, (int)size);

                timeaddr += (size+9);

                //this.Dump();

                return size + 9;
            }

            return 0;
        }




        /// <summary>
        /// 
        /// </summary>
        public override void Dispose()
        {
            while (mIsRunning) Thread.Sleep(1);
            mHisTagService = null;
            mAreaTags.Clear();

            Marshal.FreeHGlobal(mCompressedDataPointer);

            //mIsDisposed = true;
            base.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
