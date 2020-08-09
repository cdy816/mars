//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using DBRuntime.His;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class CompressEnginer : IDataCompress
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private ManualResetEvent resetEvent;

        private ManualResetEvent closedEvent;

        private Thread mCompressThread;

        private bool mIsClosed = false;

        private MergeMemoryBlock mSourceMemory;


        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, CompressMemory> mTargetMemorys = new Dictionary<int, CompressMemory>();

        private DateTime mCurrentTime;

        private IHisEngine mHisTagService;

        private long mTotalSize = 0;


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public CompressEnginer(long size)
        {
            mTotalSize = size;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 单个文件内变量的个数
        /// </summary>
        public int TagCountOneFile { get; set; } = 100000;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            CompressMemory.TagCountPerMemory = TagCountOneFile;
            foreach (var vm in mTargetMemorys)
            {
                vm.Value.Dispose();
            }
            mTargetMemorys.Clear();

            var histag = mHisTagService.ListAllTags();
            //计算数据区域个数
            var mLastDataRegionId = -1;
            foreach (var vv in histag)
            {
                var id = vv.Id;
                var did = id / TagCountOneFile;
                if (mLastDataRegionId != did)
                {
                    mTargetMemorys.Add(did, new CompressMemory() { Id = did,Name="CompressTarget"+did });
                    mLastDataRegionId = did;
                }
            }

            long psize = (long)(mTotalSize*1.3)/mTargetMemorys.Count;

            //System.Threading.Tasks.Parallel.ForEach(mTargetMemorys, (vv) => {
            //    vv.Value.ReAlloc(vv.Value.HeadSize + psize);
            //    vv.Value.Clear();
            //    LoggerService.Service.Info("CompressEnginer", "Cal CompressMemory memory size:" + (vv.Value.HeadSize + psize) / 1024.0 / 1024 + "M", ConsoleColor.Cyan);
            //});

            foreach (var vv in mTargetMemorys)
            {
                vv.Value.ReAlloc(vv.Value.HeadSize + psize);
                vv.Value.Clear();
                LoggerService.Service.Info("CompressEnginer", "Cal CompressMemory memory size:" + (vv.Value.HeadSize + psize) / 1024.0 / 1024 + "M", ConsoleColor.Cyan);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mIsClosed = false;
            mHisTagService = ServiceLocator.Locator.Resolve<IHisEngine>();

            Init();
            resetEvent = new ManualResetEvent(false);
            closedEvent = new ManualResetEvent(false);
            mCompressThread = new Thread(ThreadPro);
            mCompressThread.IsBackground = true;
            mCompressThread.Start();
           
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mIsClosed = true;
            resetEvent.Set();
            closedEvent.WaitOne();

            mSourceMemory = null;
            mHisTagService = null;

            resetEvent.Dispose();
            closedEvent.Dispose();

            foreach(var vv in mTargetMemorys)
            {
                while (vv.Value.IsBusy()) Thread.Sleep(1);
                vv.Value.Dispose();
            }
            mTargetMemorys.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataMemory"></param>
        public void RequestToCompress(MergeMemoryBlock dataMemory)
        {
            mSourceMemory = dataMemory;
            mCurrentTime = mSourceMemory.CurrentDatetime;
            foreach(var vv in mTargetMemorys)
            {
                vv.Value.CurrentTime = mCurrentTime;
            }
            resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private bool CheckIsBusy()
        {
            foreach(var vv in mTargetMemorys)
            {
                if(vv.Value.IsBusy())
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>

        private void ThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                resetEvent.Reset();
                if (mIsClosed)
                    break;

//#if DEBUG 
                Stopwatch sw = new Stopwatch();
                sw.Start();
                LoggerService.Service.Info("Compress", "********开始执行压缩********", ConsoleColor.Blue);
//#endif
                var sm = mSourceMemory;

                while (CheckIsBusy())
                {
                    LoggerService.Service.Warn("Compress", "压缩出现阻塞");
                    Thread.Sleep(500);
                }

                System.Threading.Tasks.Parallel.ForEach(mTargetMemorys, (mm) =>
                {
                    ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
                    mm.Value.Compress(sm);
                });

                ServiceLocator.Locator.Resolve<IHisEngine>().ClearMemoryHisData(sm);
                sm.MakeMemoryNoBusy();

                ServiceLocator.Locator.Resolve<IDataSerialize>().RequestToSave();

//#if DEBUG
                sw.Stop();
                LoggerService.Service.Info("Compress", ">>>>>>>>>压缩完成>>>>>>>>>" +  " ElapsedMilliseconds:" + sw.ElapsedMilliseconds, ConsoleColor.Blue);
//#endif

            }
            closedEvent.Set();

            LoggerService.Service.Info("Compress", "压缩线程退出!");

        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
