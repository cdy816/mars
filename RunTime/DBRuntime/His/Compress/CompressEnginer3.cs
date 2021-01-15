//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2021/01/14 13:39:02.
//  Version 1.0
//  种道洋
//==============================================================
using DBRuntime.His;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class CompressEnginer3 : IDataCompress3, IDisposable
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private ManualResetEvent resetEvent;

        private ManualResetEvent closedEvent;

        private Thread mCompressThread;

        private bool mIsClosed = false;

        /// <summary>
        /// 
        /// </summary>
        private Queue<HisDataMemoryBlockCollection3> mSourceMemorys = new Queue<HisDataMemoryBlockCollection3>();


        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, CompressMemory3> mTargetMemorys = new Dictionary<int, CompressMemory3>();

        //private DateTime mCurrentTime;

        private IHisEngine3 mHisTagService;

        //private long mTotalSize = 0;

        private int mLastDataRegionId;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        ///// <summary>
        ///// 
        ///// </summary>
        //public CompressEnginer2(long size)
        //{
        //    mTotalSize = size;
        //}

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
            CompressUnitManager2.Manager.Init();

            mHisTagService = ServiceLocator.Locator.Resolve<IHisEngine3>();
            CompressMemory3.TagCountPerMemory = TagCountOneFile;

            foreach (var vm in mTargetMemorys)
            {
                vm.Value.Dispose();
            }
            mTargetMemorys.Clear();

            var histag = mHisTagService.ListAllTags();
            //计算数据区域个数
            mLastDataRegionId = -1;
            foreach (var vv in histag)
            {
                var id = vv.Id;
                var did = id / TagCountOneFile;
                if (mLastDataRegionId != did)
                {
                    mTargetMemorys.Add(did, new CompressMemory3() { Id = did,Name="CompressTarget"+did });
                    mLastDataRegionId = did;
                }
            }

            foreach (var vv in mTargetMemorys)
            {
                vv.Value.Init(ServiceLocator.Locator.Resolve<IHisEngine3>().CurrentMergeMemory);
                LoggerService.Service.Info("CompressEnginer", "Cal CompressMemory memory size:" + vv.Value.Length / 1024.0 / 1024 + "M", ConsoleColor.Cyan);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        public void ReSizeTagCompress(List<HisTag> tags)
        {
            List<CompressMemory3> ctmp = new List<CompressMemory3>();
            ctmp.Add(mTargetMemorys.Last().Value);

            foreach (var vv in tags)
            {
                var id = vv.Id;
                var did = id / TagCountOneFile;
                if (mLastDataRegionId != did)
                {
                    var vvv = new CompressMemory3() { Id = did, Name = "CompressTarget" + did };
                    mTargetMemorys.Add(did, vvv);
                    mLastDataRegionId = did;
                    ctmp.Add(vvv);
                }
            }

            foreach (var vv in ctmp)
            {
                vv.ReInit(ServiceLocator.Locator.Resolve<IHisEngine3>().CurrentMergeMemory);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            LoggerService.Service.Info("CompressEnginer", "开始启动");
            mIsClosed = false;
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
            LoggerService.Service.Info("CompressEnginer", "开始停止");

            mIsClosed = true;
            resetEvent.Set();
            closedEvent.WaitOne();

            resetEvent.Dispose();
            closedEvent.Dispose();

            foreach(var vv in mTargetMemorys)
            {
                while (vv.Value.IsBusy())
                    vv.Value.DecRef();
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataMemory"></param>
        public void RequestToCompress(HisDataMemoryBlockCollection3 dataMemory)
        {
            lock(mSourceMemorys)
            mSourceMemorys.Enqueue(dataMemory);
           // mCurrentTime = dataMemory.CurrentDatetime;
            foreach(var vv in mTargetMemorys)
            {
                vv.Value.CurrentTime = dataMemory.CurrentDatetime;
                vv.Value.EndTime = dataMemory.EndDateTime;
            }
            lock (resetEvent)
                resetEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public void RequestManualToCompress(ManualHisDataMemoryBlock data)
        {
            foreach (var vv in mTargetMemorys)
            {
                if (data.Id >= vv.Value.Id * TagCountOneFile && data.Id < (vv.Value.Id + 1) * TagCountOneFile)
                {
                    vv.Value.AddManualToCompress(data);
                }
            }

        }

        /// <summary>
        /// 
        /// </summary>
        public void SubmitManualToCompress()
        {
            lock (resetEvent)
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
        /// 等待空闲
        /// </summary>
        public void WaitForReady()
        {
            while (CheckIsBusy())
            {
                Thread.Sleep(10);
            }
        }



        /// <summary>
        /// 
        /// </summary>

        private void ThreadPro()
        {
            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
            while (!mIsClosed)
            {
                try
                {
                    resetEvent.WaitOne();
                    lock (resetEvent)
                    {
                        resetEvent.Reset();
                    }

                    //#if DEBUG 
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    LoggerService.Service.Info("Compress", "********开始执行压缩********", ConsoleColor.Blue);
                    //#endif

                    if (mSourceMemorys.Count > 0)
                    {
                        while (mSourceMemorys.Count > 0)
                        {
                            HisDataMemoryBlockCollection3 sm;
                            lock (mSourceMemorys)
                                sm = mSourceMemorys.Dequeue();

                            while (CheckIsBusy())
                            {
                                LoggerService.Service.Warn("Compress", "压缩出现阻塞");
                                Thread.Sleep(500);
                            }

                            System.Threading.Tasks.Parallel.ForEach(mTargetMemorys,new ParallelOptions() { MaxDegreeOfParallelism = CPUAssignHelper.Helper.CPUArray2.Length }, (mm) =>
                            {
                                ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
                                mm.Value.Compress(sm);
                            });

                            HisDataMemoryQueryService.Service.ClearMemoryTime(sm.CurrentDatetime);
                            sm.Clear();
                            sm.MakeMemoryNoBusy();


                            System.Threading.Tasks.Parallel.ForEach(mTargetMemorys.Where(e => e.Value.HasManualCompressItems),new ParallelOptions() { MaxDegreeOfParallelism = CPUAssignHelper.Helper.CPUArray2.Length },(mm) =>
                            {
                                ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
                                mm.Value.ManualCompress();
                            });
                        }
                    }
                    else
                    {
                        System.Threading.Tasks.Parallel.ForEach(mTargetMemorys.Where(e => e.Value.HasManualCompressItems), (mm) =>
                          {
                              ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
                              mm.Value.ManualCompress();
                          });
                    }

                    //#if DEBUG
                    sw.Stop();
                    LoggerService.Service.Info("Compress", ">>>>>>>>>压缩完成>>>>>>>>>" + " ElapsedMilliseconds:" + sw.ElapsedMilliseconds, ConsoleColor.Blue);
                    //#endif

                    ServiceLocator.Locator.Resolve<IDataSerialize3>().RequestToSave();

                }
                catch(Exception ex)
                {
                    LoggerService.Service.Erro("Compress", ex.Message+ex.StackTrace);
                }

                if (mIsClosed)
                    break;
            }
            closedEvent.Set();

            LoggerService.Service.Info("Compress", "压缩线程退出!");

        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {

            foreach (var vv in mTargetMemorys)
            {
                while (vv.Value.IsBusy()) Thread.Sleep(1);
                vv.Value.Dispose();
            }
            mTargetMemorys.Clear();
            mHisTagService = null;
        }



        ///// <summary>
        ///// 
        ///// </summary>
        //private void ManualThreadPro()
        //{
        //    ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
        //    while (!mIsClosed)
        //    {
        //        mManualEvent.WaitOne();
        //        mManualEvent.Reset();
        //        if (mIsClosed)
        //            break;

        //        System.Threading.Tasks.Parallel.ForEach(mTargetMemorys.Values, (vv) =>
        //        {
        //            ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
        //            vv.MakeMemoryBusy();
        //            vv.ManualCompress();
        //            vv.MakeMemoryNoBusy();
        //        });

        //        //foreach (var vv in mTargetMemorys.Values)
        //        //{
        //        //    vv.MakeMemoryBusy();
        //        //    vv.RequestManualToCompress();
        //        //    vv.MakeMemoryNoBusy();
        //        //}

        //    }
        //}



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
