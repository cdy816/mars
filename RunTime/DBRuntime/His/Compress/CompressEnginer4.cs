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
    public class CompressEnginer4 : IDataCompress3, IDisposable, IDataCompressService
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
        private Dictionary<int, CompressMemory4> mTargetMemorys = new Dictionary<int, CompressMemory4>();

        //private DateTime mCurrentTime;

        private IHisEngine3 mHisTagService;

        //private long mTotalSize = 0;

        private int mLastDataRegionId;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...


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
            CompressMemory4.TagCountPerMemory = TagCountOneFile;

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
                    mTargetMemorys.Add(did, new CompressMemory4() { Id = did,Name="CompressTarget"+did });
                    mLastDataRegionId = did;
                }
            }

            foreach (var vv in mTargetMemorys)
            {
                vv.Value.Init(ServiceLocator.Locator.Resolve<IHisEngine3>().CurrentMergeMemory);
                LoggerService.Service.Info("CompressEnginer", "分配内存大小:" + (vv.Value.Length / 1024.0 / 1024).ToString("f1") + " M");
            }

            ServiceLocator.Locator.Registor<IDataCompressService>(this);
        }

        /// <summary>
        /// 
        /// </summary>
        public void ReInitCompress(HisDataMemoryBlockCollection3 memory)
        {
            foreach (var vv in mTargetMemorys)
            {
                if (memory != null)
                {
                    vv.Value.Init(memory);
                }
                else
                {
                    vv.Value.Init(ServiceLocator.Locator.Resolve<IHisEngine3>().CurrentMergeMemory);
                }
                LoggerService.Service.Info("CompressEnginer", "分配内存大小:" + (vv.Value.Length / 1024.0 / 1024).ToString("f1") + " M");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        public void ReSizeTagCompress(IEnumerable<int> tagids)
        {
            foreach (var vv in tagids)
            {
                var id = vv;
                var did = id / TagCountOneFile;
                if (mLastDataRegionId != did)
                {
                    var vvv = new CompressMemory4() { Id = did, Name = "CompressTarget" + did };
                    mTargetMemorys.Add(did, vvv);
                    mLastDataRegionId = did;
                }
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

            WaitForReady();
            if (!resetEvent.SafeWaitHandle.IsClosed)
            {
                //resetEvent.Set();
                closedEvent.WaitOne();

                resetEvent.Dispose();
                closedEvent.Dispose();
            }

            foreach(var vv in mTargetMemorys)
            {
                while (vv.Value.IsBusy())
                    vv.Value.DecRef();
            }
            LoggerService.Service.Info("CompressEnginer", "停止完成");
        }

        /// <summary>
        /// 压缩被阻塞
        /// </summary>
        /// <returns></returns>
        public bool IsBlocked()
        {
            return false;
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
            if (resetEvent!=null && !resetEvent.SafeWaitHandle.IsClosed)
            {
                lock (resetEvent)
                    resetEvent.Set();
            }
        }

        private bool mIsManualBusy = false;

        private object mManualLocker = new object();

        /// <summary>
        /// 提交压缩
        /// </summary>
        /// <param name="data"></param>
        public void RequestManualToCompress(ManualHisDataMemoryBlock data)
        {
            var did = data.Id / TagCountOneFile;
            if(mTargetMemorys.ContainsKey(did))
            {
                mTargetMemorys[did].AddManualToCompress(data);
                lock (mManualLocker)
                    mIsManualBusy = true;
            }
            //foreach (var vv in mTargetMemorys)
            //{
               

            //    if (data.Id >= vv.Value.Id * TagCountOneFile && data.Id < (vv.Value.Id + 1) * TagCountOneFile)
            //    {
            //        vv.Value.AddManualToCompress(data);
            //        lock(mManualLocker)
            //        mIsManualBusy = true;
            //        break;
            //    }
            //}

        }

        /// <summary>
        /// 
        /// </summary>
        public void SubmitManualToCompress()
        {
            lock (mManualLocker)
            {
                if (resetEvent != null && !resetEvent.SafeWaitHandle.IsClosed && mIsManualBusy)
                {
                    mIsManualBusy = false;
                    lock (resetEvent)
                        resetEvent.Set();
                }
            }
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
            var oldcount = mCompressExecuteCount;

            lock (resetEvent)
            {
                try
                {
                    if (resetEvent != null && !resetEvent.SafeWaitHandle.IsClosed)
                        resetEvent.Set();
                }
                catch
                {

                }
            }

            Thread.Sleep(100);

            //等待另外一个线程执行完成
            while (oldcount == mCompressExecuteCount)
            {
                if (!mCompressThread.IsAlive) break;
                Thread.Sleep(100);
            }

            while (CheckIsBusy())
            {
                Thread.Sleep(10);
            }
        }

        private int mCompressExecuteCount = 0;

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
                    if (resetEvent != null && !resetEvent.SafeWaitHandle.IsClosed)
                    {
                        resetEvent.WaitOne();
                        lock (resetEvent)
                        {
                            resetEvent.Reset();
                        }
                    }

                    //#if DEBUG 
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    LoggerService.Service.Info("Compress", "********开始执行压缩********" + " 内存占用(M): " + Process.GetCurrentProcess().WorkingSet64 / 1024.0 / 1024.0, ConsoleColor.Blue);
                    //#endif

                    if (mSourceMemorys.Count > 0)
                    {
                        while (mSourceMemorys.Count > 0)
                        {
                            HisDataMemoryBlockCollection3 sm;
                            lock (mSourceMemorys)
                                sm = mSourceMemorys.Dequeue();

                            int i = 0;
                            while (CheckIsBusy())
                            {
                                if (!mIsClosed)
                                {
                                    LoggerService.Service.Warn("Compress", "压缩出现阻塞");
                                }
                                else
                                {
                                    if(i>5)
                                    ServiceLocator.Locator.Resolve<IDataSerialize4>().RequestToSave();
                                }
                                i++;
                                Thread.Sleep(500);
                            }

                            System.Threading.Tasks.Parallel.ForEach(mTargetMemorys, new ParallelOptions() { MaxDegreeOfParallelism = CPUAssignHelper.Helper.CPUArray2.Length }, (mm) =>
                             {
                                 ThreadHelper.AssignToCPU(CPUAssignHelper.Helper.CPUArray2);
                                 mm.Value.Compress(sm);
                             });

                            HisDataMemoryQueryService3.Service.ClearMemoryTime(sm.CurrentDatetime);
                            sm.Clear();
                            sm.MakeMemoryNoBusy();


                            System.Threading.Tasks.Parallel.ForEach(mTargetMemorys.Where(e => e.Value.HasManualCompressItems), new ParallelOptions() { MaxDegreeOfParallelism = CPUAssignHelper.Helper.CPUArray2.Length }, (mm) =>
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
                        //适当的延时，防止大量的、低粒度频繁的调用压缩
                        Thread.Sleep(1000);
                    }

                    //#if DEBUG
                    sw.Stop();
                    LoggerService.Service.Info("Compress", ">>>>>>>>>压缩完成>>>>>>>>>" + " ElapsedMilliseconds:" + sw.ElapsedMilliseconds +" 内存占用(M): "+Process.GetCurrentProcess().WorkingSet64 / 1024.0 / 1024.0, ConsoleColor.Blue);
                    //#endif

                    ServiceLocator.Locator.Resolve<IDataSerialize4>().RequestToSave();
                }
                catch (Exception ex)
                {
                    LoggerService.Service.Erro("Compress", ex.Message + ex.StackTrace);
                }

                mCompressExecuteCount++;

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

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="datas"></param>
        /// <returns></returns>
        public MarshalMemoryBlock CompressData<T>(int id, DateTime startime, SortedDictionary<DateTime, T> datas, SortedDictionary<DateTime, byte> qualitys)
        {
            var did = id / TagCountOneFile;
            if(mTargetMemorys.ContainsKey(did))
            {
               return mTargetMemorys[did].CompressData<T>(id,startime,datas,qualitys);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="starttime"></param>
        /// <param name="datas"></param>
        /// <returns></returns>
        public MarshalMemoryBlock CompressData<T>(int id,DateTime starttime,HisQueryResult<T> datas)
        {
            var did = id / TagCountOneFile;
            if (mTargetMemorys.ContainsKey(did))
            {
                return mTargetMemorys[did].CompressData<T>(id, starttime,datas);
            }
            else
            {
                mTargetMemorys.Add(did, new CompressMemory4() { Id = did, Name = "CompressTarget" + did });
                mTargetMemorys[did].Init(ServiceLocator.Locator.Resolve<IHisEngine3>().CurrentMergeMemory);
                return mTargetMemorys[did].CompressData<T>(id, starttime, datas);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public void Release(MarshalMemoryBlock data)
        {
            MarshalMemoryBlockPool.Pool.Release(data);
        }

        /// <summary>
        /// 提交区域压缩
        /// </summary>
        /// <param name="data"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void RequestManualToCompress(ManualHisDataMemoryBlockArea data)
        {
            var did = data.Id / TagCountOneFile;
            if (mTargetMemorys.ContainsKey(data.Id))
            {
                mTargetMemorys[did].AddManualToCompress(data);
                lock (mManualLocker)
                    mIsManualBusy = true;
            }
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
