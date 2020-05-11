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

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class CompressMemory: MarshalMemoryBlock
    {

        #region ... Variables  ...

        private Dictionary<int, Tuple<long, int, int, int>> mTagAddress;
        private DateTime mCurrentTime;
        private IHisEngine mHisTagService;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public CompressMemory():base()
        {
            mHisTagService = ServiceLocator.Locator.Resolve<IHisEngine>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public CompressMemory(long size):base(size)
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

        /// <summary>
        /// 变量内存地址缓存
        /// Tuple 每项的含义：起始地址,值地址偏移,质量地址偏移,数据大小
        /// </summary>
        public Dictionary<int, Tuple<long, int, int, int>> TagAddress
        {
            get
            {
                return mTagAddress;
            }
            set
            {
                if (mTagAddress != value)
                {
                    mTagAddress = value;
                }
            }
        }

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

        /// <summary>
        /// 
        /// </summary>
        public void ResetTagAddress()
        {
            mTagAddress.Clear();
            mTagAddress = null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceM"></param>
        private void CheckTagAddress(MergeMemoryBlock sourceM)
        {
            if(mTagAddress==null && sourceM!=null)
            {
                mTagAddress = new Dictionary<int, Tuple<long, int, int, int>>();
                foreach (var vv in sourceM.TagAddress.Where(e=>e.Key>=Id* TagCountPerMemory && e.Key<(Id+1)* TagCountPerMemory))
                {
                    mTagAddress.Add(vv.Key,vv.Value);
                }
            }
        }

        /// <summary>
        /// 执行压缩
        /// </summary>
        public void Compress(MergeMemoryBlock source)
        {
            /*
             内存结构:Head+数据指针区域+数据区
             Head:数据区大小(4)+变量数量(4)
             数据区指针:[ID(4) + address(4)]
             数据区:[data block]
             */

            Stopwatch sw = new Stopwatch();
            sw.Start();

             CheckTagAddress(source);
            long datasize = 0;
            int headOffset = 4 + 4;
            long Offset = headOffset + this.mTagAddress.Count * 8;

            
            Dictionary<int, long> dtmp = new Dictionary<int, long>();

            this.MakeMemoryBusy();

            //更新数据区域
            foreach(var vv in mTagAddress)
            {
                var size = CompressBlockMemory(source, vv.Value.Item1, Offset,vv.Value.Item3, vv.Value.Item4, vv.Key);
                dtmp.Add(vv.Key, Offset);
                Offset += size;
                datasize += size;
            }

            //更新指针区域
            this.WriteInt(0,(int)datasize);
            this.Write((int)this.mTagAddress.Count);

            int count = 0;
            foreach (var vv in dtmp)
            {
                this.WriteInt(headOffset + count, (int)vv.Key);
                this.WriteInt(headOffset + count+4, (int)vv.Value);
                count+=8;
            }

            ServiceLocator.Locator.Resolve<IDataSerialize>().RequestToSeriseFile(this, mCurrentTime);
            sw.Stop();

            LoggerService.Service.Info("CompressEnginer", Id+ "压缩完成 耗时:"+sw.ElapsedMilliseconds);
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
        private long CompressBlockMemory(MergeMemoryBlock mSourceMemory, long addr, long targetPosition,long qulityOffset, long len, int id)
        {
            //var qulityoffset = mSourceMemory.ReadInt(addr);
            // var id = mSourceMemory.ReadInt(addr + 4);

            var histag = mHisTagService.GetHisTag(id);

            if (histag == null) return 0;

            var qulityoffset = qulityOffset;

            var comtype = histag.CompressType;//压缩类型

            this.CheckAndResize(targetPosition + len);

            //写入压缩类型
            this.WriteByte(targetPosition + 4, (byte)comtype);

            var tp = CompressUnitManager.Manager.GetCompress(comtype);
            if (tp != null)
            {
                tp.QulityOffset = (int)qulityoffset;
                tp.TagType = histag.TagType;
                tp.RecordType = histag.Type;
                tp.StartTime = mCurrentTime;
                tp.Parameters = histag.Parameters;
                var size = tp.Compress(mSourceMemory, addr, this, targetPosition + 5, len) + 1;
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
            mHisTagService = null;
            mTagAddress.Clear();
            mTagAddress = null;
            base.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
