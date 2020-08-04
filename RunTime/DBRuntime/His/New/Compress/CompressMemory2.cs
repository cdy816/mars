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
    public class CompressMemory2: MarshalMemoryBlock
    {

        #region ... Variables  ...

        //private Dictionary<int, HisDataMemoryBlock> mTagAddress;
        private DateTime mCurrentTime;
        private IHisEngine2 mHisTagService;
        private Dictionary<int, CompressUnitbase2> mCompressCach = new Dictionary<int, CompressUnitbase2>();
        private Dictionary<int, long> dtmp = new Dictionary<int, long>();

        private List<int> mTagIds=new List<int>();

        //private bool mIsDisposed=false;
        private bool mIsRunning=false;

       
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


        #endregion ...Properties...

        #region ... Methods    ...

        ///// <summary>
        ///// 
        ///// </summary>
        //public void ResetTagAddress()
        //{
        //    mTagAddress.Clear();
        //    mTagAddress = null;
        //    mCompressCach.Clear();
        //}

        public void Init(HisDataMemoryBlockCollection sourceM)
        {
            mTagIds.Clear();
            long lsize = 0;
            foreach (var vv in sourceM.TagAddress.Where(e => e.Key >= Id * TagCountPerMemory && e.Key < (Id + 1) * TagCountPerMemory))
            {
                mTagIds.Add(vv.Key);
                dtmp.Add(vv.Key, 0);
                var cpt = mHisTagService.GetHisTag(vv.Key).CompressType;
                if (!mCompressCach.ContainsKey(cpt))
                {
                    mCompressCach.Add(cpt, CompressUnitManager2.Manager.GetCompressQuick(cpt).Clone());
                }
                lsize += vv.Value.Length;
            }

            this.ReAlloc(HeadSize + (long)(lsize*1.2));
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
                lsize += vv.Value.Length;
            }

            this.Resize(HeadSize + lsize);
        }

        /// <summary>
        /// 执行压缩
        /// </summary>
        public void Compress(HisDataMemoryBlockCollection source)
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
                    var size = CompressBlockMemory(val,  Offset, val.QualityAddress, val.Length, vv);
                    if(dtmp.ContainsKey(vv))
                    dtmp[vv] = Offset;
                    Offset += size;
                    datasize += size;
                }

                //更新指针区域
                this.WriteInt(0, (int)datasize);
                this.Write((int)source.TagAddress.Count);

                long ltmp2 = sw.ElapsedMilliseconds;

                int count = 0;
                foreach (var vv in dtmp)
                {
                    this.WriteInt(headOffset + count, (int)vv.Key);
                    this.WriteInt(headOffset + count + 4, (int)vv.Value);
                    count += 8;
                }

                long ltmp3 = sw.ElapsedMilliseconds;

                ServiceLocator.Locator.Resolve<IDataSerialize2>().RequestToSeriseFile(this, mCurrentTime);
                sw.Stop();
                LoggerService.Service.Info("CompressEnginer", Id + "压缩完成 耗时:" + sw.ElapsedMilliseconds + " ltmp1:" + ltmp1 + " ltmp2:" + (ltmp2 - ltmp1) + " ltmp3:" + (ltmp3 - ltmp2) +" CPU Id:"+ThreadHelper.GetCurrentProcessorNumber(), ConsoleColor.Blue);
                
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("CompressEnginer", ex.StackTrace+"  "+ex.Message);
            }
            mIsRunning = false;
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

          //  this.CheckAndResize(targetPosition + len);

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
