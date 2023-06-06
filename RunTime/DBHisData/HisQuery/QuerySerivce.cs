//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Xml.Linq;
using Cdy.Tag.Interface;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class QuerySerivce : IHisQuery,IDisposable
    {
        IHisQueryFromMemory mMemoryService;

        StatisticsFileHelper statisticsHelper;


        //private Dictionary<string,object> mFileLastValueCach = new Dictionary<string,object>();

        //private Dictionary<string, object> mFileFirstValueCach = new Dictionary<string, object>();


        /// <summary>
        /// 
        /// </summary>
        public int FileDuration { get; set; } = 4;

        /// <summary>
        /// 
        /// </summary>
        public int BlockDuration { get; set; } = 5;

        /// <summary>
        /// 
        /// </summary>
        public QuerySerivce()
        {
            mMemoryService = ServiceLocator.Locator.Resolve<IHisQueryFromMemory>() as IHisQueryFromMemory;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="databaseName"></param>
        public QuerySerivce(string databaseName) : this()
        {
            Database = databaseName;
            statisticsHelper = new StatisticsFileHelper() { Database = databaseName, Manager = GetFileManager() };
        }

        public string Database { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private DataFileManager GetFileManager()
        {
            return HisQueryManager.Instance.GetFileManager(Database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private bool IsCanQueryFromMemory()
        {
            return mMemoryService != null;
        }


        ///// <summary>
        ///// 读取某个时间之前的第一个有效值
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="id"></param>
        ///// <param name="datetime"></param>
        ///// <returns></returns>
        //public object ReadLastAvaiableValue<T>(int id,DateTime datetime,QueryContext context)
        //{
        //    object tobj=null;
        //    string tmp = Database + ((int)(id / 100000)).ToString("X3") + "{0}{1}{2}04{3}.dbd2";

        //    //var dtt = context.LastTime.Date.AddHours((int)(context.LastTime.Hour / FileDuration) * FileDuration);
        //    //if(dtt == datetime)
        //    //{
        //    //    return context.LastValue;
        //    //}

        //    //从当前文件文件中读取
        //    string sfile = DataFileInfo6Extend.ListDataFile2(datetime, tmp);
        //    if(!string.IsNullOrEmpty(sfile))
        //    {
        //        var obj = context.GetLastFileKeyHisValueRegistor(sfile);
        //        if(obj != null)
        //        {
        //            TagHisValue<T> tt = (TagHisValue<T>)obj;
        //            if(!tt.IsEmpty())
        //            return obj;
        //        }
        //        else
        //        {
        //            DataFileInfo6 dfile = new DataFileInfo6() { FileName = sfile, IsZipFile = sfile.EndsWith(DataFileManager.ZipDataFile2Extends) };
        //            using (var dsf = dfile.GetFileSeriser())
        //            {
        //                for (int i = 47; i >= 0; i--)
        //                {
        //                    tobj = dfile.ReadLastAvaiableValue2<T>(dsf, id, i, context, out bool needcancel);
        //                    if (tobj != null)
        //                    {
        //                        break;
        //                    }
        //                    else if (needcancel)
        //                    {
        //                        break;
        //                    }
        //                }
        //            }
        //            if (tobj != null)
        //            {
        //                context.RegistorLastFileKeyHisValue<T>(sfile, tobj);
        //                return obj;
        //            }
        //            else
        //            {
        //                context.RegistorLastFileKeyHisValue<T>(sfile, TagHisValue<T>.Empty);
        //            }
        //        }
        //    }

        //    //从之前的文件中读取
        //    foreach (var vv in DataFileInfo6Extend.ListPreviewDataFiles2(datetime,tmp))
        //    {
        //        var obj = context.GetLastFileKeyHisValueRegistor(vv);
        //        if (obj != null)
        //        {
        //            TagHisValue<T> tt = (TagHisValue<T>)obj;
        //            if(tt.IsEmpty())
        //            {
        //                continue;
        //            }
        //            else
        //            {
        //                return obj;
        //            }
        //        }
        //        else
        //        {
        //            DataFileInfo6 dfile = new DataFileInfo6() { FileName = vv, IsZipFile = vv.EndsWith(DataFileManager.ZipDataFile2Extends) };
        //            using (var dsf = dfile.GetFileSeriser())
        //            {
        //                for (int i = 47; i >= 0; i--)
        //                {
        //                    tobj = dfile.ReadLastAvaiableValue2<T>(dsf, id, i, context, out bool needcancel);
        //                    if (tobj != null)
        //                    {
        //                        break;
        //                    }
        //                    else if (needcancel)
        //                    {
        //                        break;
        //                    }
        //                }
        //            }
        //            if(tobj!=null)
        //            {
        //                context.RegistorLastFileKeyHisValue<T>(vv,tobj);
        //            }
        //            else
        //            {
        //                context.RegistorLastFileKeyHisValue<T>(vv,TagHisValue<T>.Empty);
        //            }
        //        }


        //    }
        //    return tobj;
        //}

        ///// <summary>
        ///// 读取某个时间之后的第一个有效值
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="datetime"></param>
        ///// <returns></returns>
        //public object ReadFirstAvaiableValue<T>(int id,DateTime datetime,QueryContext context)
        //{
        //    object tobj = null;
        //    string tmp = Database + ((int)(id / 100000)).ToString("X3") + "{0}{1}{2}04{3}.dbd2";
        //    foreach (var vv in DataFileInfo6Extend.ListNextDataFiles2(datetime, tmp))
        //    {
        //        var obj = context.GetFirstFileKeyHisValueRegistor(vv);
        //        if (obj != null)
        //        {
        //            TagHisValue<T> tt = (TagHisValue<T>)obj;
        //            if (tt.IsEmpty())
        //            {
        //                continue;
        //            }
        //            else
        //            {
        //                return obj;
        //            }
        //        }
        //        else
        //        {
        //            DataFileInfo6 dfile = new DataFileInfo6() { FileName = vv, IsZipFile = vv.EndsWith(DataFileManager.ZipDataFile2Extends) };
        //            using (var dsf = dfile.GetFileSeriser())
        //            {
        //                for (int i = 0; i <= 47; i++)
        //                {
        //                    tobj = dfile.ReadFirstAvaiableValue2<T>(dsf, id, i, context, out bool needcancel);
        //                    if (tobj != null)
        //                    {
        //                        break;
        //                    }
        //                    else if (needcancel)
        //                    {
        //                        break;
        //                    }
        //                }
        //            }
        //            if (tobj != null)
        //            {
        //                context.RegistorFirstFileKeyHisValue<T>(vv, tobj);
        //            }
        //            else
        //            {
        //                context.RegistorFirstFileKeyHisValue<T>(vv, TagHisValue<T>.Empty);
        //            }
        //        }
        //    }
        //    return tobj;
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            ReadValueByUTCTimeInner<T>(id, times.Select(e => e.ToUniversalTime()), type, result);
            result.ConvertUTCTimeToLocal();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValueIgnorClosedQuality<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            ReadValueByUTCTimeInner<T>(id, times.Select(e => e.ToUniversalTime()), type, result, true);
            result.ConvertUTCTimeToLocal();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValueByUTCTimeIgnorClosedQuality<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            ReadValueByUTCTimeInner<T>(id, times, type, result, true);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValueByUTCTime<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            ReadValueByUTCTimeInner<T>(id, times, type, result);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <param name="ignorClosedQuality"></param>
        private void ReadValueByUTCTimeInner<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result,bool ignorClosedQuality=false)
        {
            Stopwatch sw = Stopwatch.StartNew();
            sw.Start();

            QueryContext ctx = new QueryContext();
            ctx.IgnorCloseQuality= ignorClosedQuality;

            ctx.Add("IHisQuery", this);

            Dictionary<DateTime, int> mtimes = new Dictionary<DateTime, int>();

            int i = 0;
            //foreach (var item in times)
            //{
            //    mtimes.Add(item, i);
            //    i++;
            //}

            long ttmp0 = sw.ElapsedMilliseconds;

            TimeValueDictionary mFileTimes = new TimeValueDictionary();
            List<DateTime> mMemoryTimes = new List<DateTime>();
            List<DateTime> mEmptyTimes = new List<DateTime>();

            List<DateTime> mAHeadTimes = new List<DateTime>();

            var fm = GetFileManager();

            string sname = Database + (id / fm.TagCountOneFile);
            //判断数据是否在内存中
            if (IsCanQueryFromMemory())
            {
                mMemoryService.LockMemoryFile();
                var vtime = mMemoryService.GetMemoryTimer(id);

                foreach (var vv in times)
                {
                    try
                    {
                        if (vtime != null)
                        {
                            if (vv >= vtime.Item1 && vv < vtime.Item2)
                            {
                                mMemoryTimes.Add(vv);
                            }
                            else
                            {
                                //if (vv >= vtime.Item2 || fm.CheckDataInLogFile(vv, sname))
                                if (vv >= vtime.Item2)
                                {
                                    mAHeadTimes.Add(vv);
                                   // mEmptyTimes.Add(vv);
                                }
                                else
                                {
                                    mFileTimes.AppendTime(vv);
                                }
                            }
                        }
                        else
                        {
                            if (fm.CheckDataInLogFile(vv, sname))
                            {
                                mEmptyTimes.Add(vv);
                            }
                            else
                            {
                                mFileTimes.AppendTime(vv);
                            }
                        }
                        //if (!mMemoryService.CheckTime(id, vv,out bool isgreat))
                        //{
                        //    //ltmp.AppendTime(vv);
                        //    if (isgreat || fm.CheckDataInLogFile(vv, sname))
                        //    {
                        //        mLogTimes.Add(vv);
                        //    }
                        //    else
                        //    {
                        //        ltmp.AppendTime(vv);
                        //    }
                        //}
                        //else
                        //{
                        //    mMemoryTimes.Add(vv);
                        //}
                    }
                    catch
                    {

                    }
                    result.FillDatetime(vv, i);
                    mtimes.Add(vv, i);
                    i++;
                }
            }
            else
            {
                foreach(var vv in times)
                {
                    try
                    {
                        if (fm.CheckDataInLogFile(vv, sname))
                        {
                            mEmptyTimes.Add(vv);
                        }
                        else
                        {
                            mFileTimes.AppendTime(vv);
                        }
                    }
                    catch
                    {

                    }
                    //mtimes.Add(vv, i);
                    result.FillDatetime(vv,i);
                    mtimes.Add(vv, i);
                    i++;
                }
            }

            result.TimeIndex = mtimes;
            //result.FillDatetime();
            result.FillQuality();

            long ttmp1 = sw.ElapsedMilliseconds;

            var vfiles = fm.GetDataFiles(mFileTimes, mEmptyTimes, id);

            string msg = "";
            if (mMemoryTimes.Count > 0)
            {
                msg = $"从内存读取数据个数 {mMemoryTimes.Count} 时间范围: {mMemoryTimes[0].ToLocalTime()}  -- {mMemoryTimes[mMemoryTimes.Count - 1].ToLocalTime()};";
            }
            else
            {
                msg = "从内存读取数据个数为空;";
            }

            if(mFileTimes.Count>0)
            {
                msg += $"从文件读取个数 {mFileTimes.Count} 时间范围:{mFileTimes.First().Value.First().Value.First().ToLocalTime()} -- {mFileTimes.Last().Value.Last().Value.Last().ToLocalTime()};";
            }
            else
            {
                msg += "从文件读取数据个数为空;";
            }


            if (mAHeadTimes.Count > 0)
            {
                msg += $"超出最新时间拟合个数 {mAHeadTimes.Count} 时间范围: {mAHeadTimes.First().ToLocalTime()} -- {mAHeadTimes.Last().ToLocalTime()};";
            }
            else
            {
                msg += "超出最新时间拟合个数为空;";
            }

            if(mEmptyTimes.Count > 0)
            {
                msg += $"空数据拟合个数:{mEmptyTimes.Count}";
            }

            LoggerService.Service.Info("QueryService", msg);

            //var vfiles = GetFileManager().GetDataFiles(ltmp, mLogTimes, id);

            ////介于内存时间和存盘的最后时间之间的时间
            //List<DateTime> mMemoryDiskTimes = new List<DateTime>();

            //if (IsCanQueryFromMemory() && vfiles.Count > 0 && mMemoryTimes.Count > 0)
            //{
            //    foreach (var vv in mLogTimes.Where(e => e < mMemoryTimes[0] && e > vfiles.First().Key))
            //    {
            //        mMemoryDiskTimes.Add(vv);
            //    }
            //    foreach (var vv in mMemoryDiskTimes)
            //    {
            //        mLogTimes.Remove(vv);
            //    }
            //}

            IDataFile mPreFile = null;
            IDataFile mLastFile = null;
            List<DateTime> mtime = new List<DateTime>();

            //从历史文件中读取数据
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        ctx.CurrentFile = mPreFile.FileName;

                        if (mPreFile is HisDataFileInfo4)
                        {
                            (mPreFile as HisDataFileInfo4).Read(id, mtime, type, result);
                        }
                        else if (mPreFile is DataFileInfo4)
                            (mPreFile as DataFileInfo4).Read<T>(id, mtime, type, result);
                        else if (mPreFile is DataFileInfo6)
                        {
                            (mPreFile as DataFileInfo6).Read(id, mtime, type, result,ctx);
                        }
                        mLastFile = mPreFile;
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add(default(T), vv.Key, (byte)QualityConst.Null);
                }
                else if (!vv.Value.Equals(mPreFile))
                {
                    if (mPreFile != null)
                    {
                        ctx.CurrentFile = mPreFile.FileName;

                        if (mPreFile is HisDataFileInfo4) (mPreFile as HisDataFileInfo4).Read(id, mtime, type, result);
                        else if (mPreFile is DataFileInfo4)
                            (mPreFile as DataFileInfo4).Read<T>(id, mtime, type, result);
                        else if (mPreFile is DataFileInfo6)
                        {
                            (mPreFile as DataFileInfo6).Read<T>(id, mtime, type, result, ctx);
                        }
                        mLastFile = mPreFile;
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
                else
                {
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                ctx.CurrentFile = mPreFile.FileName;

                if (mPreFile is HisDataFileInfo4) (mPreFile as HisDataFileInfo4).Read(id, mtime, type, result);
                else if (mPreFile is DataFileInfo4)
                    (mPreFile as DataFileInfo4).Read<T>(id, mtime, type, result);
                else if (mPreFile is DataFileInfo6)
                {
                    (mPreFile as DataFileInfo6).Read<T>(id, mtime, type, result, ctx);
                }
                mLastFile = mPreFile;
            }

            long ttmp2 = sw.ElapsedMilliseconds;

            if (IsCanQueryFromMemory())
            {
                //if (mMemoryDiskTimes.Count > 0 && mLastFile!=null)
                //{
                //    FillMemoryDiskTimeValue(id, mMemoryDiskTimes, result, mLastFile,ctx, type);
                //}

                //从内存中读取数据
                ReadFromMemory(id, mMemoryTimes, type, result,ctx,out DateTime dnow);

                FillAHeadValue(id, mAHeadTimes, type,  ctx,result);

                //填充空的数据
                FillNoneValue<T>(id, mEmptyTimes, type, ctx, result, dnow);

                mMemoryService.UnLockMemoryFile();
            }
            else
            {
                //填充空的数据
                FillNoneValue<T>(id, mEmptyTimes, type,ctx, result,null);
            }

            ctx.Dispose();

            result.TimeIndex = null;
            sw.Stop();
#if DEBUG
            Debug.Print($"ReadValueByUTCTime 读取 {times.Count()} 个历史数据耗时 初始化:{ttmp1} 从文件读取:{ttmp2 - ttmp1} 从内存读取,填充空值:{sw.ElapsedMilliseconds - ttmp2} 总耗时:{sw.ElapsedMilliseconds} ms ");
#endif
            LoggerService.Service.Info("QueryService", $"ReadValueByUTCTime 读取 {times.Count()} 个历史数据耗时 初始化:{ttmp1} 从文件读取:{ ttmp2-ttmp1} 从内存读取,填充空值:{sw.ElapsedMilliseconds - ttmp2} 总耗时:{ sw.ElapsedMilliseconds} ms ");

            //LogHisQueryResult(result);
        }

        //private void LogHisQueryResult<T>(HisQueryResult<T> result)
        //{
        //    var ss = new System.IO.StreamWriter(System.IO.File.OpenWrite(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location) + "\\" + DateTime.Now.Ticks.ToString() + ".txt"));
        //    for(int i=0;i<result.Count;i++)
        //    {
        //        var val = result.GetValue(i, out DateTime time, out byte qua);
        //        ss.WriteLine($"{i} = {time} {val} {qua}");
        //    }
        //    ss.Close();
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="times"></param>
        private void FillMemoryDiskTimeValue<T>(int id,List<DateTime> times, HisQueryResult<T> result,IDataFile file,QueryContext ctx, QueryValueMatchType type)
        {
            TagHisValue<T>? val = null;
            //object vtmp = ctx.GetLastFileKeyHisValueRegistor(file.FileName);
            //if(vtmp == null)
            //{
                var vtmp = (file as DataFileInfo6).ReadFileLastAvaiableValue<T>(id, ctx);
                if (vtmp != null)
                {
                    val = (TagHisValue<T>)vtmp;
                }
                else
                {
                    val = null;
                }
            //}
            //else
            //{
            //    val = (TagHisValue<T>)vtmp;
            //}

            var memorylastValue = mMemoryService.GetStartValue<T>(id, out DateTime time, out byte qualiry);
            foreach (var vtime in times)
            {
                switch (type)
                {
                    case QueryValueMatchType.Previous:
                        if (val.HasValue)
                        {
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                        }
                        else
                        {
                            result.Add(default(T), vtime, (byte)QualityConst.Null);
                        }
                        break;
                    case QueryValueMatchType.After:
                        if (memorylastValue!=null)
                        {
                            result.Add(memorylastValue, vtime, qualiry);
                        }
                        else
                        {
                            result.Add(default(T), vtime, (byte)QualityConst.Null);
                        }
                        break;
                    case QueryValueMatchType.Linear:
                        if (val.HasValue && memorylastValue!=null)
                        {
                            if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (time - vtime).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {

                                    result.Add(memorylastValue, vtime, qualiry);
                                }
                            }
                            else
                            {
                                if (!IsBadQuality(qualiry) && !IsBadQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (time - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = memorylastValue;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    result.Add((object)val1, vtime, (pval1 / tval1)>0.5 ? qualiry : val.Value.Quality);
                                }
                                else if (!IsBadQuality(qualiry))
                                {
                                    result.Add(mMemoryService, vtime, qualiry);
                                }
                                else if (!IsBadQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(default(T), vtime, (byte)QualityConst.Null);
                                }
                            }
                        }
                        else
                        {
                            result.Add(default(T), vtime, (byte)QualityConst.Null);
                        }
                        break;
                    case QueryValueMatchType.Closed:
                        if (val.HasValue)
                        {
                            var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                            var fval = (time - vtime).TotalMilliseconds;

                            if (ppval < fval)
                            {
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            }
                            else
                            {
                                result.Add(memorylastValue, vtime, qualiry);
                            }
                        }
                        else
                        {
                            result.Add(default(T), vtime, (byte)QualityConst.Null);
                        }
                        break;
                }
            }

        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="id"></param>
        ///// <param name="times"></param>
        ///// <param name="type"></param>
        ///// <param name="result"></param>
        //public  void FillNoneValues<T>(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result,QueryContext context,DateTime timelimit)
        //{
        //    if (times.Count > 0)
        //    {
        //        SortedDictionary<DateTime, List<DateTime>> dtmps = new SortedDictionary<DateTime, List<DateTime>>();

        //        foreach (DateTime dt in times)
        //        {
        //            var dtt = dt.Date.AddHours((int)(dt.Hour / FileDuration) * FileDuration);
        //            if (dtmps.ContainsKey(dtt))
        //            {
        //                dtmps[dtt].Add(dt);
        //            }
        //            else
        //            {
        //                dtmps.Add(dtt, new List<DateTime>() { dt });
        //            }
        //        }

        //        foreach (var vv in dtmps)
        //        {
        //            if (type == QueryValueMatchType.Previous)
        //            {
        //                var vobj = ReadLastAvaiableValue<T>(id, vv.Key,context);
        //                if (vobj != null)
        //                {
        //                    TagHisValue<T> hval = (TagHisValue<T>)vobj;
        //                    if(hval.Quality == (byte)QualityConst.Close)
        //                    {
        //                        foreach (var vvv in vv.Value)
        //                        {
        //                            result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                        }
        //                    }
        //                    else
        //                    {
        //                        foreach (var vvv in vv.Value)
        //                        {
        //                            if(vvv<=timelimit)
        //                            result.Add(hval.Value, vvv, hval.Quality);
        //                            else
        //                            {
        //                                result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                            }
        //                        }
        //                    }
                            
        //                }
        //                else
        //                {
        //                    foreach (var vvv in vv.Value)
        //                    {
        //                        result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                    }
        //                }
        //            }
        //            else if (type == QueryValueMatchType.After)
        //            {
        //                var vobj = ReadFirstAvaiableValue<T>(id, vv.Key, context);
        //                if (vobj != null)
        //                {
        //                    TagHisValue<T> hval = (TagHisValue<T>)vobj;
        //                    foreach (var vvv in vv.Value)
        //                    {
        //                        if (vvv <= timelimit)
        //                            result.Add(hval.Value, vvv, hval.Quality);
        //                        else
        //                            result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                    }
        //                }
        //                else
        //                {
        //                    foreach (var vvv in vv.Value)
        //                    {
        //                        result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                    }
        //                }
        //            }
        //            else if (type == QueryValueMatchType.Closed)
        //            {
        //                var pobj = ReadLastAvaiableValue<T>(id, vv.Key, context);
        //                var nobj = ReadFirstAvaiableValue<T>(id, vv.Key, context);
        //                if (pobj != null && nobj != null)
        //                {
        //                    TagHisValue<T> hval = (TagHisValue<T>)pobj;
        //                    TagHisValue<T> nval = (TagHisValue<T>)nobj;

        //                    if (hval.Quality == (byte)QualityConst.Close)
        //                    {
        //                        foreach (var vvv in vv.Value)
        //                        {
        //                            if(vvv<= timelimit)
        //                            result.Add(nval.Value, vvv, nval.Quality);
        //                            else
        //                            result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                        }
        //                    }
        //                    else
        //                    {
        //                        foreach (var vvv in vv.Value)
        //                        {
        //                            if (vvv <= timelimit)
        //                            {
        //                                if ((vvv - hval.Time).TotalMinutes > (nval.Time - vvv).TotalMinutes)
        //                                {
        //                                    result.Add(nval.Value, vvv, nval.Quality);
        //                                }
        //                                else
        //                                {
        //                                    result.Add(hval.Value, vvv, hval.Quality);
        //                                }
        //                            }
        //                            else
        //                            {
        //                                result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                            }
        //                        }
        //                    }
        //                }
        //                else if (pobj != null)
        //                {
        //                    TagHisValue<T> hval = (TagHisValue<T>)pobj;
        //                    foreach (var vvv in vv.Value)
        //                    {
        //                        if (vvv <= timelimit)
        //                            result.Add(hval.Value, vvv, hval.Quality);
        //                        else
        //                        {
        //                            result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                        }
        //                    }
        //                }
        //                else if (nobj != null)
        //                {
        //                    TagHisValue<T> hval = (TagHisValue<T>)nobj;
        //                    foreach (var vvv in vv.Value)
        //                    {
        //                        if (vvv <= timelimit)
        //                            result.Add(hval.Value, vvv, hval.Quality);
        //                        else
        //                            result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                    }
        //                }
        //                else
        //                {
        //                    foreach (var vvv in vv.Value)
        //                    {
        //                        result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                    }
        //                }
        //            }
        //            else if (type == QueryValueMatchType.Linear)
        //            {
        //                var pobj = ReadLastAvaiableValue<T>(id, vv.Key, context);
        //                var nobj = ReadFirstAvaiableValue<T>(id, vv.Key, context);
        //                if (pobj != null && nobj != null)
        //                {
        //                    TagHisValue<T> hval = (TagHisValue<T>)pobj;
        //                    TagHisValue<T> nval = (TagHisValue<T>)nobj;
        //                    var tval = (nval.Time - hval.Time).TotalSeconds;

        //                    if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
        //                    {
        //                        foreach (var vvv in vv.Value)
        //                        {
        //                            var ppval = (vvv - hval.Time).TotalMilliseconds;
        //                            var ffval = (nval.Time - vvv).TotalMilliseconds;
        //                            if (vvv <= timelimit)
        //                            {
        //                                if (ppval < ffval)
        //                                {
        //                                    result.Add(hval.Value, vvv, hval.Quality);
        //                                }
        //                                else
        //                                {
        //                                    result.Add(nval.Value, vvv, nval.Quality);
        //                                }
        //                            }
        //                            else
        //                            {
        //                                result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                            }
        //                        }
        //                    }
        //                    else
        //                    {
        //                        if ((!IsBadQuality(hval.Quality)) && (!IsBadQuality(nval.Quality)))
        //                        {
        //                            foreach (var vvv in vv.Value)
        //                            {
        //                                if (vvv > timelimit)
        //                                {
        //                                    result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                                    continue;
        //                                }
        //                                    var pval1 = (hval.Time - vvv).TotalMilliseconds;
        //                                var tval1 = (nval.Time - vvv).TotalMilliseconds;
        //                                var sval1 = hval.Value;
        //                                var sval2 = nval.Value;

        //                                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

        //                                string tname = typeof(T).Name;
        //                                //if (vv <= dnow)
        //                                {
        //                                    switch (tname)
        //                                    {
        //                                        case "Byte":
        //                                            result.Add((byte)val1, vvv, 0);
        //                                            break;
        //                                        case "Int16":
        //                                            result.Add((short)val1, vvv, 0);
        //                                            break;
        //                                        case "UInt16":
        //                                            result.Add((ushort)val1, vvv, 0);
        //                                            break;
        //                                        case "Int32":
        //                                            result.Add((int)val1, vvv, 0);
        //                                            break;
        //                                        case "UInt32":
        //                                            result.Add((uint)val1, vvv, 0);
        //                                            break;
        //                                        case "Int64":
        //                                            result.Add((long)val1, vvv, 0);
        //                                            break;
        //                                        case "UInt64":
        //                                            result.Add((ulong)val1, vvv, 0);
        //                                            break;
        //                                        case "Double":
        //                                            result.Add((double)val1, vvv, 0);
        //                                            break;
        //                                        case "Single":
        //                                            result.Add((float)val1, vvv, 0);
        //                                            break;
        //                                    }
        //                                }

        //                            }
        //                        }
        //                        else if (!IsBadQuality(hval.Quality))
        //                        {
        //                            foreach (var vvv in vv.Value)
        //                            {
        //                                if (vvv > timelimit)
        //                                {
        //                                    result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                                    continue;
        //                                }
        //                                result.Add(hval.Value, vvv, hval.Quality);
        //                            }
        //                        }
        //                        else if (!IsBadQuality(nval.Quality))
        //                        {
        //                            foreach (var vvv in vv.Value)
        //                            {
        //                                if (vvv > timelimit)
        //                                {
        //                                    result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                                    continue;
        //                                }
        //                                result.Add(nval.Value, vvv, nval.Quality);
        //                            }
        //                        }
        //                        else
        //                        {
        //                            foreach (var vvv in vv.Value)
        //                            {
        //                                result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                            }
        //                        }

        //                    }
        //                }
        //                else
        //                {
        //                    foreach (var vvv in vv.Value)
        //                    {
        //                        result.Add(default(T), vvv, (byte)QualityConst.Null);
        //                    }
        //                }
        //            }
        //        }

        //        //foreach(var vv in mLogTimes)
        //        //{
        //        //    result.Add(default(T), vv, (byte)QualityConst.Null);
        //        //}
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="qa"></param>
        ///// <returns></returns>
        //public static bool IsBadQuality(byte qa)
        //{
        //    return qa >= (byte)QualityConst.Bad && qa <= (byte)QualityConst.Bad + 20;
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private void ReadFromMemory<T>(int id,List<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result,QueryContext context,out DateTime timelimite)
        {
            DateTime dt = DateTime.Now;
            mMemoryService?.ReadValue<T>(id, times, type, result,context,out dt);
            timelimite = dt;
        }

        private DateTime GetTimeKey(DateTime time)
        {
            return new DateTime(time.Year, time.Month, time.Day, ((int)(time.Hour / FileDuration))*FileDuration, 0, 0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="mTimes"></param>
        /// <param name="type"></param>
        /// <param name="context"></param>
        /// <param name="result"></param>
        private void FillAHeadValue<T>(int id, List<DateTime> mTimes, QueryValueMatchType type, QueryContext context, HisQueryResult<T> result)
        {
            //if (type != QueryValueMatchType.Previous) return;
            
            if (mTimes.Count == 0) return;

            object mlastvalue = context.ContainsKey("MemoryLastValue") ? context["MemoryLastValue"] : null;
            byte mlastquality = context.ContainsKey("MemoryLastQuality") ? (byte)context["MemoryLastQuality"] : (byte)0;

            if(mlastvalue==null)
            {
                var preval = this.ReadFileLastValue<T>(id, mTimes[0], context);
                TagHisValue<T>? pval = null;
                if (preval != null)
                    pval = (TagHisValue<T>)preval;
                if (mTimes.Count > 0)
                {
                    foreach (var vv in mTimes)
                    {
                        result.Add(pval.Value.Value, vv, pval.Value.Quality);
                    }
                }
            }
            else
            {
                if (mTimes.Count > 0)
                {
                    foreach (var vv in mTimes)
                    {
                        result.Add(mlastvalue, vv, mlastquality);
                    }
                }
            }
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="mLogTimes"></param>
        /// <param name="result"></param>
        private void FillNoneValue<T>(int id, List<DateTime> mTimes, QueryValueMatchType type,QueryContext context, HisQueryResult<T> result,DateTime? nowtime)
        {
            //if (!context.IgnorCloseQuality)
            //{
            //    if (mTimes.Count > 0)
            //    {
            //        foreach (var vv in mTimes)
            //        {
            //            result.Add(default(T), vv, (byte)QualityConst.Null);
            //        }
            //    }
            //}
            //else
            {

                //将在一个文件内的规整在一起
                SortedDictionary<DateTime, List<DateTime>> mtimecaches = new SortedDictionary<DateTime, List<DateTime>>();

                foreach(var vv in mTimes)
                {
                    var vkey = GetTimeKey(vv);
                    if(mtimecaches.ContainsKey(vkey))
                    {
                        mtimecaches[vkey].Add(vv);
                    }
                    else
                    {
                        mtimecaches.Add(vkey, new List<DateTime>() { vv });
                    }
                }

                object mlastvalue = context.ContainsKey("MemoryLastValue") ? context["MemoryLastValue"] : null;
                byte mlastquality = context.ContainsKey("MemoryLastQuality") ?(byte)context["MemoryLastQuality"] : (byte)0;

                foreach (var vv in mtimecaches)
                {
                    var preval = this.ReadFileLastValue<T>(id, vv.Key, context);
                    var nxtval = this.ReadFileFirstValue<T>(id, vv.Key.AddHours(FileDuration), context);
                    TagHisValue<T>? pval = null;
                    if (preval != null)
                        pval = (TagHisValue<T>)preval;
                    TagHisValue<T>? nval = null;
                    if (nxtval != null)
                        nval = (TagHisValue<T>)nxtval;

                    switch (type)
                    {
                        case QueryValueMatchType.Previous:

                            if (pval.HasValue)
                            {
                                var squa = (pval.Value.Quality == (byte)QualityConst.Close || pval.Value.Quality == (byte)QualityConst.Start) ? (byte)0 : pval.Value.Quality;
                                foreach (var vvv in vv.Value)
                                {
                                    if (nowtime != null && mlastvalue!=null && vvv>=nowtime)
                                    {
                                        result.Add(mlastvalue, vvv, mlastquality);
                                    }
                                    else
                                    {
                                        result.Add(pval.Value.Value, vvv, squa);
                                    }
                                }
                            }
                            else
                            {
                                foreach (var vvv in vv.Value)
                                {
                                    if (nowtime != null && mlastvalue != null && vvv >= nowtime)
                                    {
                                        result.Add(mlastvalue, vvv, mlastquality);
                                    }
                                    else
                                    {
                                        result.Add(default(T), vvv, (byte)QualityConst.Null);
                                    }
                                }

                            }
                            break;
                        case QueryValueMatchType.After:
                            if (nval.HasValue)
                            {
                                var squa = (nval.Value.Quality == (byte)QualityConst.Close || nval.Value.Quality == (byte)QualityConst.Start) ? (byte)0 : nval.Value.Quality;
                                foreach (var vvv in vv.Value)
                                {
                                    result.Add(nval.Value.Value, vvv, squa);
                                }
                            }
                            else
                            {
                                foreach (var vvv in vv.Value)
                                {
                                    result.Add(default(T), vvv, (byte)QualityConst.Null);
                                }

                            }
                            break;
                        case QueryValueMatchType.Linear:
                            if (pval.HasValue && nval.HasValue)
                            {
                                var squa = (pval.Value.Quality == (byte)QualityConst.Close || pval.Value.Quality == (byte)QualityConst.Start) ? (byte)0 : pval.Value.Quality;
                                var equa = (nval.Value.Quality == (byte)QualityConst.Close || nval.Value.Quality == (byte)QualityConst.Start) ? (byte)0 : nval.Value.Quality;

                                if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                                {

                                    foreach (var vvv in vv.Value)
                                    {
                                        if (nowtime != null && mlastvalue != null && vvv >= nowtime)
                                        {
                                            result.Add(mlastvalue, vvv, mlastquality);
                                        }
                                        else
                                        {
                                            if ((vvv - pval.Value.Time) > (nval.Value.Time - vvv))
                                            {
                                                result.Add(nval.Value.Value, vvv, equa);
                                            }
                                            else
                                            {
                                                result.Add(pval.Value.Value, vvv, squa);
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    var sval1 = pval.Value.Value;
                                    var sval2 = nval.Value.Value;

                                    foreach (var vvv in vv.Value)
                                    {
                                        if (nowtime != null && mlastvalue != null && vvv >= nowtime)
                                        {
                                            result.Add(mlastvalue, vvv, mlastquality);
                                        }
                                        else
                                        {
                                            var pval1 = (vvv - pval.Value.Time).TotalMilliseconds;
                                            var tval1 = (nval.Value.Time - pval.Value.Time).TotalMilliseconds;
                                            double vval = Convert.ToDouble(sval2) - Convert.ToDouble(sval1);
                                            double val1;
                                            if (vval == 0)
                                            {
                                                val1 = Convert.ToDouble(sval2);
                                            }
                                            else
                                            {
                                                val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);
                                            }
                                            if (pval1 <= 0)
                                            {
                                                //说明数据有异常，则取第一个值
                                                result.Add((object)sval1, vvv, pval.Value.Quality);
                                            }
                                            else
                                            {
                                                result.Add((object)val1, vvv, (pval1 / tval1) < 0.5 ? squa : equa);
                                            }
                                        }
                                    }
                                }
                            }
                            else if (pval.HasValue)
                            {
                                var squa = (pval.Value.Quality == (byte)QualityConst.Close || pval.Value.Quality == (byte)QualityConst.Start) ? (byte)0 : pval.Value.Quality;
                                foreach (var vvv in vv.Value)
                                {
                                    if (nowtime != null && mlastvalue != null && vvv >= nowtime)
                                    {
                                        result.Add(mlastvalue, vvv, mlastquality);
                                    }
                                    else
                                    {
                                        result.Add(pval.Value.Value, vvv, squa);
                                    }
                                }
                            }
                            else if (nval.HasValue)
                            {
                                var equa = (nval.Value.Quality == (byte)QualityConst.Close || nval.Value.Quality == (byte)QualityConst.Start) ? (byte)0 : nval.Value.Quality;
                                foreach (var vvv in vv.Value)
                                {
                                    if (nowtime != null && mlastvalue != null && vvv >= nowtime)
                                    {
                                        result.Add(mlastvalue, vvv, mlastquality);
                                    }
                                    else
                                    {
                                        result.Add(nval.Value.Value, vvv, equa);
                                    }
                                }
                            }
                            else
                            {
                                foreach (var vvv in vv.Value)
                                {
                                    if (nowtime != null && mlastvalue != null && vvv >= nowtime)
                                    {
                                        result.Add(mlastvalue, vvv, mlastquality);
                                    }
                                    else
                                    {
                                        result.Add(default(T), vvv, (byte)QualityConst.Null);
                                    }
                                }
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            if (pval.HasValue && nval.HasValue)
                            {
                                var squa = (pval.Value.Quality == (byte)QualityConst.Close || pval.Value.Quality == (byte)QualityConst.Start) ? (byte)0 : pval.Value.Quality;
                                var equa = (nval.Value.Quality == (byte)QualityConst.Close || nval.Value.Quality == (byte)QualityConst.Start) ? (byte)0 : nval.Value.Quality;
                                foreach (var vvv in vv.Value)
                                {
                                    if (nowtime != null && mlastvalue != null && vvv >= nowtime)
                                    {
                                        result.Add(mlastvalue, vvv, mlastquality);
                                    }
                                    else
                                    {
                                        if ((vvv - pval.Value.Time) > (nval.Value.Time - vvv))
                                        {
                                            result.Add(nval.Value.Value, vvv, equa);
                                        }
                                        else
                                        {
                                            result.Add(pval.Value.Value, vvv, squa);
                                        }
                                    }
                                }
                            }
                            else if (pval.HasValue)
                            {
                                var squa = (pval.Value.Quality == (byte)QualityConst.Close || pval.Value.Quality == (byte)QualityConst.Start) ? (byte)0 : pval.Value.Quality;
                                foreach (var vvv in vv.Value)
                                {
                                    if (nowtime != null && mlastvalue != null && vvv >= nowtime)
                                    {
                                        result.Add(mlastvalue, vvv, mlastquality);
                                    }
                                    else
                                    {
                                        result.Add(pval.Value.Value, vvv, squa);
                                    }
                                }
                            }
                            else if (nval.HasValue)
                            {
                                var equa = (nval.Value.Quality == (byte)QualityConst.Close || nval.Value.Quality == (byte)QualityConst.Start) ? (byte)0 : nval.Value.Quality;
                                foreach (var vvv in vv.Value)
                                {
                                    if (nowtime != null && mlastvalue != null && vvv >= nowtime)
                                    {
                                        result.Add(mlastvalue, vvv, mlastquality);
                                    }
                                    else
                                    {
                                        result.Add(nval.Value.Value, vvv, equa);
                                    }
                                }
                            }
                            else
                            {
                                foreach (var vvv in vv.Value)
                                {
                                    if (nowtime != null && mlastvalue != null && vvv >= nowtime)
                                    {
                                        result.Add(mlastvalue, vvv, mlastquality);
                                    }
                                    else
                                    {
                                        result.Add(default(T), vvv, (byte)QualityConst.Null);
                                    }
                                }
                            }
                            break;
                    }

                }

                
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="id"></param>
        ///// <param name="mLogTimes"></param>
        ///// <param name="result"></param>
        //private void ReadLogFile<T>(int id, List<DateTime> mLogTimes, QueryValueMatchType type, HisQueryResult<T> result)
        //{
        //    if (mLogTimes.Count > 0)
        //    {
        //        List<DateTime> mtime = new List<DateTime>();
        //        var lfiles = GetFileManager().GetLogDataFiles(mLogTimes);

        //        LogFileInfo mPlFile = null;
        //        DateTime dnow = DateTime.UtcNow;
        //        foreach (var vv in lfiles)
        //        {
        //            if (vv.Key > dnow)
        //            {
        //                break;
        //            }
        //            if (vv.Value == null)
        //            {
        //                if (mPlFile != null)
        //                {
        //                    mPlFile.Read<T>(id, mtime, type, result);
        //                    mPlFile = null;
        //                    mtime.Clear();
        //                }
        //                result.Add(default(T), vv.Key, (byte)QualityConst.Null);
        //            }
        //            else if (vv.Value != mPlFile)
        //            {
        //                if (mPlFile != null)
        //                {
        //                    mPlFile.Read<T>(id, mtime, type, result);
        //                }
        //                mPlFile = vv.Value;
        //                mtime.Clear();
        //                mtime.Add(vv.Key);
        //            }
        //            else
        //            {
        //                mtime.Add(vv.Key);
        //            }
        //        }
        //        if (mPlFile != null)
        //        {
        //            mPlFile.Read<T>(id, mtime, type, result);
        //        }
        //    }
        //}



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        private void ReadLogFileAllValue<T>(int id,DateTime startTime,DateTime endTime,HisQueryResult<T> result)
        {
            var vfiles = GetFileManager().GetLogDataFiles(startTime,endTime);
            vfiles.ForEach(e => {
                DateTime sstart = e.StartTime > startTime ? e.StartTime : startTime;
                DateTime eend = e.EndTime > endTime ? endTime : endTime;
                e.ReadAllValue<T>(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="starttime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        private void ReadAllValueFromMemory<T>(int id,DateTime starttime,DateTime endTime,HisQueryResult<T> result)
        {
            mMemoryService?.ReadAllValue(id, starttime, endTime, result);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue<T>(int id, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            ReadAllValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), result);
            result.ConvertUTCTimeToLocal();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            try
            {

                DateTime etime = endTime,stime = startTime;
                DateTime memoryTime = DateTime.MaxValue;
                if(IsCanQueryFromMemory())
                {
                    memoryTime = mMemoryService.GetStartMemoryTime(id);
                }

                if(startTime>=memoryTime)
                {
                    ReadAllValueFromMemory(id, startTime, endTime, result);
                }
                else
                {
                    var fileMananger = GetFileManager();

                    ////优先从日志中读取历史记录
                    //memoryTime = fileMananger.LastLogTime > memoryTime ? fileMananger.LastLogTime : memoryTime;

                    if (endTime>memoryTime)
                    {
                        etime = memoryTime;
                    }

                    Tuple<DateTime, DateTime> mLogFileTimes;
                    var vfiles = fileMananger.GetDataFiles(stime, etime, out mLogFileTimes, id);
                    //ltmp0 = sw.ElapsedMilliseconds;
                    //从历史记录中读取数据
                    foreach(var e in vfiles)
                    {
                        DateTime sstart = e.StartTime > startTime ? e.StartTime : startTime;
                        DateTime eend = e.EndTime > endTime ? endTime : e.EndTime;
                        if (e is HisDataFileInfo4)
                        {
                            (e as HisDataFileInfo4).ReadAllValue(id, startTime, endTime, result);
                        }
                        else if (e is DataFileInfo4) { (e as DataFileInfo4).ReadAllValue(id, sstart, eend, result); }
                        else if (e is DataFileInfo6) { (e as DataFileInfo6).ReadAllValue(id, sstart, eend, result); }
                    }

                    //从日志文件中读取数据
                    if (mLogFileTimes.Item1 < mLogFileTimes.Item2)
                    {
                        ReadLogFileAllValue(id, mLogFileTimes.Item1, mLogFileTimes.Item2, result);
                    }

                    //从内存中读取数据
                    if(endTime>memoryTime)
                    {
                        ReadAllValueFromMemory(id, memoryTime, endTime, result);
                    }
                    //ltmp2 = sw.ElapsedMilliseconds;
                }

                //sw.Stop();

                //Debug.Print("ReadAllValueByUTCTime "+ ltmp0 +" , " +(ltmp1-ltmp0)+" , "+(ltmp2-ltmp1));
                
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("QueryService", ex.StackTrace);
            }
        }

        /// <summary>
        /// 原始值过滤查询
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="filter"></param>
        /// <param name="tagtypes"></param>
        /// <returns></returns>
        public HisQueryTableResult ReadAllValueAndFilter(List<int> ids, DateTime startTime, DateTime endTime, ExpressFilter filter, Dictionary<int, byte> tagtypes)
        {
            var result = ReadAllValueByUTCTimeAndFilter(ids, startTime.ToUniversalTime(), endTime.ToUniversalTime(), filter, tagtypes);
            return result.ConvertUTCTimeToLocal();
        }

        /// <summary>
        /// 原始值过滤查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public HisQueryTableResult ReadAllValueByUTCTimeAndFilter(List<int> ids, DateTime startTime, DateTime endTime,ExpressFilter filter,Dictionary<int,byte> tagtypes)
        {
            try
            {
                DateTime dt = new DateTime(startTime.Year, startTime.Month, startTime.Day, startTime.Hour, 0, 0);
                dt.AddMinutes((startTime.Minute/BlockDuration)*BlockDuration);

                List<Tuple<DateTime, DateTime>> timespans = new List<Tuple<DateTime, DateTime>>();
                DateTime dts = startTime;
                DateTime dtt = dt.AddMinutes(BlockDuration);
                dtt = dtt>endTime?endTime:dtt;
                timespans.Add(new Tuple<DateTime, DateTime>(dts, dtt));

                while (dtt<endTime)
                {
                    dts = dtt;
                    dtt = dtt.AddMinutes(BlockDuration);
                    dtt = dtt > endTime ? endTime : dtt;
                    timespans.Add(new Tuple<DateTime, DateTime>(dts, dtt));
                }

                HisQueryTableResult result = new HisQueryTableResult();
                foreach(var id in ids)
                {
                    result.AddColumn(id.ToString(), (TagType)tagtypes[id]);
                }
                result.Init((int)((endTime - startTime).TotalSeconds));

                foreach (var vv in timespans)
                {
                    ReadAllValueByUTCTimeAndFilterInner(ids, vv.Item1, vv.Item2, filter, tagtypes, result);
                }
                return result;
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("QueryService", ex.StackTrace);
            }
            return null;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="filter"></param>
        /// <param name="tagtypes"></param>
        private void ReadAllValueByUTCTimeAndFilterInner(List<int> ids, DateTime startTime, DateTime endTime, ExpressFilter filter, Dictionary<int, byte> tagtypes, HisQueryTableResult result)
        {
            MultiHisQueryResult htr = new MultiHisQueryResult();
            using (QueryContextBase ctx = new QueryContextBase())
            {
                foreach (var vv in tagtypes)
                {
                    switch(vv.Value)
                    {
                        case (byte)TagType.Bool:
                            htr.AddColumn(vv.Key.ToString(),ReadAllValueByUTCTime<bool>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.Byte:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<byte>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.Short:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<Int16>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.UShort:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<UInt16>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.Int:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<Int32>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.UInt:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<UInt32>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.Long:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<Int64>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.ULong:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<UInt64>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.Double:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<double>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.Float:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<float>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.String:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<string>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.DateTime:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<DateTime>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.IntPoint:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<IntPointData>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.UIntPoint:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<UIntPointData>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.IntPoint3:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<IntPoint3Data>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.UIntPoint3:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<UIntPoint3Data>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.LongPoint:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<LongPointData>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.ULongPoint:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<ULongPointData>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.LongPoint3:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<LongPoint3Data>(vv.Key, startTime, endTime));
                            break;
                        case (byte)TagType.ULongPoint3:
                            htr.AddColumn(vv.Key.ToString(), ReadAllValueByUTCTime<ULongPoint3Data>(vv.Key, startTime, endTime));
                            break;
                    }
                }
            }
            foreach(var vvv in htr.ListVallValues())
            {
                if (vvv == null) break;
                if(filter.IsFit(vvv))
                {
                    result.CheckAndResize();
                    result.Add((DateTime)vvv.First().Value, vvv.Skip(1).Select(e => e.Value).ToArray());
                }
            }
            htr.Dispose();
        }



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public HisQueryResult<T> ReadAllValue<T>(int id, DateTime startTime, DateTime endTime)
        {
            return ReadAllValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime()).ConvertUTCTimeToLocal();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public HisQueryResult<T> ReadAllValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime)
        {
            int valueCount = (int)(endTime - startTime).TotalSeconds;
            var result = new HisQueryResult<T>(valueCount);
            ReadAllValueByUTCTime(id, startTime, endTime, result);
            return result as HisQueryResult<T>;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public HisQueryResult<T> ReadValue<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type)
        {
            return ReadValueByUTCTime<T>(id, times.Select(e => e.ToUniversalTime()), type).ConvertUTCTimeToLocal();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public HisQueryResult<T> ReadValueIgnorClosedQuality<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type)
        {
            return ReadValueByUTCTimeIgnorClosedQuality<T>(id, times.Select(e => e.ToUniversalTime()), type).ConvertUTCTimeToLocal();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public HisQueryResult<T> ReadValueByUTCTime<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type)
        {
            int valueCount = times.Count();

            var result = new HisQueryResult<T>(valueCount);
            ReadValueByUTCTime(id, times, type, result);
            return result; 
        }


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public HisQueryResult<T> ReadValueByUTCTimeIgnorClosedQuality<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type)
        {
            int valueCount = times.Count();

            var result = new HisQueryResult<T>(valueCount);
            ReadValueByUTCTimeIgnorClosedQuality(id, times, type, result);
            return result;
        }

        /// <summary>
        /// 读取某个时间段内，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult ReadNumberStatistics(int id, DateTime startTime, DateTime endTime)
        {
            return ReadNumberStatisticsByUTCTime(id, startTime.ToUniversalTime(), endTime.ToUniversalTime()).ConvertUTCTimeToLocal();
        }

        /// <summary>
        /// 读取某个时间段（UTC时间）内，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult ReadNumberStatisticsByUTCTime(int id, DateTime startTime, DateTime endTime)
        {
            return statisticsHelper.Read(id, startTime, endTime);
        }

        /// <summary>
        /// 读取指定时间点的，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult ReadNumberStatistics(int id, IEnumerable<DateTime> times)
        {
            return ReadNumberStatisticsByUTCTime(id, times.Select(e => e.ToUniversalTime())).ConvertUTCTimeToLocal();
        }

        /// <summary>
        /// 读取指定时间点（UTC时间）的，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult ReadNumberStatisticsByUTCTime(int id, IEnumerable<DateTime> times)
        {
            return statisticsHelper.Read(id, times);
        }



        #region Number Tag Value Statistics

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public Tuple<DateTime,object> FindNumberTagValue<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type)
        {
            return FindNumberTagValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), para,para2, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public Dictionary<DateTime,object> FindNumberTagValues<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type)
        {
            return FindNumberTagValuesByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), para,para2, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public double FindNumberTagValueDuration<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type)
        {
            return FindNumberTagValueDurationByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), para,para2, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public double FindNumberTagMaxMinValue<T>(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, out IEnumerable<DateTime> time)
        {
            return FindNumberTagMaxMinValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), type,out time);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public double FindNumberTagAvgValue<T>(int id, DateTime startTime, DateTime endTime)
        {
            return FindNumberTagAvgValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime());
        }


        /// <summary>
        /// 查找数字型变量的的等于指定值的时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="para2"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public Tuple<DateTime,object> FindNumberTagValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime, double para,double para2,NumberStatisticsType type)
        {
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
            double dmin = para - para2;
            double dmax = para + para2;

            for (int i = 0; i < vals.Count; i++)
            {
                var tmp = vals.GetValue(i, out DateTime time, out byte qu);
                var val = Convert.ToDouble(tmp);
                if (IsGoodQuality(qu))
                {
                    switch(type)
                    {
                        case NumberStatisticsType.EqualsValue:
                            if(val == para || (val >= dmin && val <= dmax))
                            {
                                return new Tuple<DateTime, object>(time,tmp);
                            }
                            break;
                        case NumberStatisticsType.GreatValue:
                            if (val > para)
                            {
                                return new Tuple<DateTime, object>(time, tmp);
                            }
                            break;
                        case NumberStatisticsType.LowValue:
                            if (val < para)
                            {
                                return new Tuple<DateTime, object>(time, tmp);
                            }
                            break;
                    }
                }
            }
            return new Tuple<DateTime, object>(DateTime.MinValue,default(T));
        }

        /// <summary>
        /// 查找数字型变量的的等于指定值的时间集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>

        public Dictionary<DateTime,object> FindNumberTagValuesByUTCTime<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type)
        {
            Dictionary<DateTime,object> re = new Dictionary<DateTime, object>();
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);

            double dmin = para - para2;
            double dmax = para + para2;

            for (int i = 0; i < vals.Count; i++)
            {
                T tmp = vals.GetValue(i, out DateTime time, out byte qu);
                var val = Convert.ToDouble(tmp);
                if (IsGoodQuality(qu))
                {
                    switch (type)
                    {
                        case NumberStatisticsType.EqualsValue:
                            if (val == para || (val>= dmin && val<=dmax))
                            {
                                re.Add(time,tmp);
                            }
                            break;
                        case NumberStatisticsType.GreatValue:
                            if (val > para)
                            {
                                re.Add(time,tmp);
                            }
                            break;
                        case NumberStatisticsType.LowValue:
                            if (val < para)
                            {
                                re.Add(time,tmp);
                            }
                            break;
                    }
                   
                }
            }
            return re;
        }

        /// <summary>
        /// 查找数字型变量的的等于指定值得保持时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="para2"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public double FindNumberTagValueDurationByUTCTime<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type)
        {
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
            double tim = 0;
            bool ishase = false;
            DateTime statetime = DateTime.MinValue;
            double dmin = para - para2;
            double dmax = para + para2;
            for (int i = 0; i < vals.Count; i++)
            {
                var val = Convert.ToDouble(vals.GetValue(i, out DateTime time, out byte qu));

                switch (type)
                {
                    case NumberStatisticsType.EqualsValue:
                        if (IsGoodQuality(qu) && (val == para|| (val >= dmin && val <= dmax)))
                        {
                            if (!ishase)
                            {
                                ishase = true;
                                statetime = time;
                            }
                        }
                        else
                        {
                            if (ishase)
                            {
                                ishase = false;
                                tim += (time - statetime).TotalSeconds;
                            }
                        }
                        break;
                    case NumberStatisticsType.GreatValue:
                        if (IsGoodQuality(qu) && val > para)
                        {
                            if (!ishase)
                            {
                                ishase = true;
                                statetime = time;
                            }
                        }
                        else
                        {
                            if (ishase)
                            {
                                ishase = false;
                                tim += (time - statetime).TotalSeconds;
                            }
                        }
                        break;
                    case NumberStatisticsType.LowValue:
                        if (IsGoodQuality(qu) && val < para)
                        {
                            if (!ishase)
                            {
                                ishase = true;
                                statetime = time;
                            }
                        }
                        else
                        {
                            if (ishase)
                            {
                                ishase = false;
                                tim += (time - statetime).TotalSeconds;
                            }
                        }
                        break;
                }

                
            }

            if (ishase)
            {
                vals.GetValue(vals.Count - 1, out DateTime time, out byte qua);
                tim += (time - statetime).TotalSeconds;
            }
            return tim;
        }

        /// <summary>
        /// 查找最大、最小值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public double FindNumberTagMaxMinValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime,NumberStatisticsType type,out IEnumerable<DateTime> time)
        {
            List<DateTime> ret = new List<DateTime>();
            var hh = (endTime - startTime).TotalHours;
            DateTime retime = DateTime.MinValue;
            if (hh > 2 && (type == NumberStatisticsType.Max || type == NumberStatisticsType.Min))
            {
                //从统计数据文件中读取值，加快统计数据的读取
                var etime1 = new DateTime(startTime.Year, startTime.Month, startTime.Day, startTime.Hour + 1, 0, 0);
                var etime2 = new DateTime(endTime.Year, endTime.Month, endTime.Day, endTime.Hour, 0, 0);
                var val1 = ReadAllValueByUTCTime<T>(id, startTime, etime1);
                var val3 = ReadAllValueByUTCTime<T>(id, etime2, endTime);

                var vvs = ReadNumberStatisticsByUTCTime(id, etime1, etime2);

                switch (type)
                {
                    case NumberStatisticsType.Max:
                        double re = StatisticsValue<T>(val1, type,out List<DateTime> time1);
                        ret = time1;
                        var mv2 = StatisticsValue<T>(val3, type,out List<DateTime> tim2);
                       
                        if(re == mv2)
                        {
                            ret.AddRange(tim2);
                        }
                        else
                        {
                            re = Math.Max(re, mv2);
                            if(re==mv2)
                            {
                                ret = tim2;
                            }
                        }

                        if (vvs != null)
                        {
                            foreach (var vv in vvs.ListAllValue())
                            {
                                if (re == vv.MaxValue)
                                {
                                    ret.Add(vv.MaxTime);
                                }
                                else
                                {
                                    re = Math.Max(re, vv.MaxValue);
                                    if (re == vv.MaxValue)
                                    {
                                        ret = new List<DateTime>() { vv.MaxTime };
                                    }
                                }
                            }
                        }
                        time = ret;
                        return re;
                    case NumberStatisticsType.Min:
                        re = StatisticsValue<T>(val1, type, out List<DateTime> time3);
                        ret = time3;
                        mv2 = StatisticsValue<T>(val3, type, out List<DateTime> tim4);

                        if (re == mv2)
                        {
                            ret.AddRange(tim4);
                        }
                        else
                        {
                            re = Math.Min(re, mv2);
                            if (re == mv2)
                            {
                                ret = tim4;
                            }
                        }

                        if (vvs != null)
                        {
                            foreach (var vv in vvs.ListAllValue())
                            {
                                if (re == vv.MinValue)
                                {
                                    ret.Add(vv.MinTime);
                                }
                                else
                                {
                                    re = Math.Min(re, vv.MinValue);
                                    if (re == vv.MinValue)
                                    {
                                        ret = new List<DateTime>() { vv.MinTime };
                                    }
                                }
                            }
                        }
                        time = ret;
                        return re;
                    default:
                        time = ret;
                        return 0;
                }

            }
            else
            {
                var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
                var re = StatisticsValue(vals, type,out ret);
                time = ret;
                return re;
            }
        }

        /// <summary>
        /// 获取一段时间内的平均值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public double FindNumberTagAvgValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime)
        {
            List<DateTime> ret = new List<DateTime>();
            var hh = (endTime - startTime).TotalHours;
            DateTime retime = DateTime.MinValue;
            if (hh > 2)
            {
                //从统计数据文件中读取值，加快统计数据的读取
                var etime1 = new DateTime(startTime.Year, startTime.Month, startTime.Day, startTime.Hour + 1, 0, 0);
                var etime2 = new DateTime(endTime.Year, endTime.Month, endTime.Day, endTime.Hour, 0, 0);
                var val1 = ReadAllValueByUTCTime<T>(id, startTime, etime1);
                var val3 = ReadAllValueByUTCTime<T>(id, etime2, endTime);

                var vvs = ReadNumberStatisticsByUTCTime(id, etime1, etime2);

                double re = 0;
                var ttime = (endTime - startTime).TotalSeconds;
                double mv1 = StatisticsValue<T>(val1, NumberStatisticsType.Avg, out List<DateTime> time);
                if (mv1 != double.MinValue)
                {
                    re += mv1 * ((etime1 - startTime).TotalSeconds);
                }
                else
                {
                    ttime -= (etime1 - startTime).TotalSeconds;
                }
                double mv2 = StatisticsValue<T>(val3, NumberStatisticsType.Avg, out List<DateTime> time2);

                if (mv2 != double.MinValue)
                {
                    re += mv2 * ((endTime - etime2).TotalSeconds);
                }
                else
                {
                    ttime -= (endTime - etime2).TotalSeconds;
                }

                if (vvs != null)
                {
                    foreach (var vv in vvs.ListAllValue())
                    {
                        if (vv.AvgValue != double.MinValue)
                        {
                            re += vv.AvgValue * 3600;
                        }
                        else
                        {
                            ttime -= 3600;
                        }

                    }
                }
                if (ttime > 0)
                {
                    re = re / ttime;
                    return re;
                }
                else
                {
                    return double.MinValue;
                }

            }
            else
            {
                var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
                var re = StatisticsValue(vals,NumberStatisticsType.Avg, out ret);
                return re;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="vals"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        private double StatisticsValue<T>(HisQueryResult<T> vals, NumberStatisticsType type, out List<DateTime> datetime)
        {
            double re = 0;
            List<DateTime> ret = new List<DateTime>();
            switch (type)
            {
                case NumberStatisticsType.Max:
                    re = double.MinValue;
                    for (int i = 0; i < vals.Count; i++)
                    {
                        var val = Convert.ToDouble(vals.GetValue(i, out DateTime time, out byte qu));
                        if (IsGoodQuality(qu))
                        {
                            re = Math.Max(val, re);
                            if (re == val)
                            {
                                ret.Add(time);
                            }
                        }
                    }
                    break;
                case NumberStatisticsType.Min:
                    re = double.MaxValue;
                    for (int i = 0; i < vals.Count; i++)
                    {
                        var val = Convert.ToDouble(vals.GetValue(i, out DateTime time, out byte qu));
                        if (IsGoodQuality(qu))
                        {
                            re = Math.Min(val, re);
                            if (re == val)
                            {
                                ret.Add(time);
                            }
                        }
                    }
                    break;
                case NumberStatisticsType.Avg:
                    re = double.MinValue;
                    int count = 0;
                    for (int i = 0; i < vals.Count; i++)
                    {
                        var val = Convert.ToDouble(vals.GetValue(i, out DateTime time, out byte qu));
                        if (IsGoodQuality(qu))
                        {
                            re += val;
                            count++;
                        }
                    }
                    if (count > 0)
                        re = re / count;
                    break;


            }
            datetime = ret;
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="qa"></param>
        /// <returns></returns>
        protected bool IsBadQuality(byte qa)
        {
            return (qa >= (byte)QualityConst.Bad && qa <= (byte)QualityConst.Bad + 20) || qa == (byte)QualityConst.Close;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="qu"></param>
        /// <returns></returns>
        bool IsGoodQuality(byte qu)
        {
            return !IsBadQuality(qu);
        }

        #endregion


        #region No Number Tag Statistics

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public double FindNoNumberTagValueDuration<T>(int id, DateTime startTime, DateTime endTime,  object para)
        {
            return FindNoNumberTagValueDurationByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(),  para);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public DateTime FindNoNumberTagValue<T>(int id, DateTime startTime, DateTime endTime, object para)
        {
            return FindNoNumberTagValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), para);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public List<DateTime> FindNoNumberTagValues<T>(int id, DateTime startTime, DateTime endTime, object para)
        {
            return FindNoNumberTagValuesByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), para);
        }

        /// <summary>
        /// 查找指定的值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public DateTime FindNoNumberTagValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime, object para)
        {
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
            for (int i = 0; i < vals.Count; i++)
            {
                if ((object)vals.GetValue(i,out DateTime time,out byte qu) == para)
                {
                    if(IsGoodQuality(qu))
                    return time;
                }
            }
            return DateTime.MinValue;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public List<DateTime> FindNoNumberTagValuesByUTCTime<T>(int id, DateTime startTime, DateTime endTime,  object para)
        {
            List<DateTime> re = new List<DateTime>();
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
            for (int i = 0; i < vals.Count; i++)
            {
                if ((object)vals.GetValue(i, out DateTime time, out byte qu) == para)
                {
                    if (IsGoodQuality(qu))
                    {
                        re.Add(time);
                    }
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public double FindNoNumberTagValueDurationByUTCTime<T>(int id, DateTime startTime, DateTime endTime,  object para)
        {
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
            double tim = 0;
            bool ishase = false;
            DateTime statetime = DateTime.MinValue;

            for (int i = 0; i < vals.Count; i++)
            {

                if ((object)vals.GetValue(i, out DateTime time, out byte qua) == para)
                {
                    if (!ishase)
                    {
                        ishase = true;
                        statetime = time;
                    }
                }
                else
                {
                    if (ishase)
                    {
                        ishase = false;
                        tim += (time - statetime).TotalSeconds;
                    }
                }
            }

            if (ishase)
            {
                vals.GetValue(vals.Count - 1, out DateTime time, out byte qua);
                tim += (time - statetime).TotalSeconds;
            }
            return tim;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void ModifyHisData<T>(int id, HisQueryResult<T> values, string user, string msg)
        {
            Dictionary<DataFileInfo6,List<DateTime>> dic = new Dictionary<DataFileInfo6,List<DateTime>>();
            Dictionary<DateTime,List<DateTime>> dic2 = new Dictionary<DateTime, List<DateTime>>();

            DateTime stime=DateTime.MinValue, etime=DateTime.MinValue;

            for(int i=0;i< values.Count; i++)
            {
                var vv = values.GetValue(i,out DateTime time,out byte qua);

                if(i==0) stime = time;
                else if(i == values.Count-1)
                {
                    etime = time;
                }

                var vdata = new DateTime(time.Year, time.Month, time.Day, ((int)(time.Hour / FileDuration))*FileDuration, 0, 0);
                if (dic2.ContainsKey(vdata))
                {
                    dic2[vdata].Add(time);
                }
                else
                {
                    dic2.Add(vdata, new List<DateTime>() { time });
                }
            }

            foreach(var vdata in dic2)
            {
                var vfile = ServiceLocator.Locator.Resolve<IHisDataManagerService>().GetHisFileName(id, vdata.Key);

                if (!System.IO.File.Exists(vfile))
                {
                    var vv = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(vfile), System.IO.Path.GetFileNameWithoutExtension(vfile))+ DataFileManager.ZipDataFile2Extends;
                    if (System.IO.File.Exists(vv))
                    {
                        vfile = vv;
                    }
                }

                var vdd = new DateTime(vdata.Key.Year, vdata.Key.Month, vdata.Key.Day, ((int)(vdata.Key.Hour / FileDuration)) * FileDuration, 0, 0);
                dic.Add(new DataFileInfo6() { FileName = vfile,Duration = new TimeSpan(this.FileDuration,0,0),FileDuration = FileDuration,BlockDuration = BlockDuration, StartTime= vdd, IsZipFile = System.IO.Path.GetExtension(vfile) == DataFileManager.ZipDataFile2Extends },vdata.Value);
            }

            foreach(var vdata in dic)
            {
                vdata.Key.ModifyHisData<T>(id, stime, etime, values);
            }
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void DeleteHisData<T>(int id, DateTime starttime, DateTime endtime, string user, string msg)
        {
            Dictionary<DataFileInfo6, Tuple<DateTime,DateTime>> dic = new Dictionary<DataFileInfo6, Tuple<DateTime, DateTime>>();

            Dictionary<DateTime, DateTime> dic2 = new Dictionary<DateTime, DateTime>();
            DateTime dtmp = starttime;
            while(dtmp<endtime)
            {
                var vdata = new DateTime(dtmp.Year, dtmp.Month, dtmp.Day, ((int)(dtmp.Hour / FileDuration))*FileDuration, 0, 0);
                var edata = vdata.AddHours(FileDuration);
                if(edata>endtime)
                {
                    dic2.Add(dtmp, endtime);
                    break;
                }
                else
                {
                    dic2.Add(dtmp, edata);
                }
                dtmp = edata;
            }

            foreach (var vdata in dic2)
            {
                var vfile = ServiceLocator.Locator.Resolve<IHisDataManagerService>().GetHisFileName(id, vdata.Key);

                if(!System.IO.File.Exists(vfile))
                {
                    var vv = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(vfile), System.IO.Path.GetFileNameWithoutExtension(vfile)) + DataFileManager.ZipDataFile2Extends;
                    if (System.IO.File.Exists(vv))
                    {
                        vfile = vv;
                    }
                }

                if (System.IO.File.Exists(vfile))
                {
                    var vdd = new DateTime(vdata.Key.Year, vdata.Key.Month, vdata.Key.Day, ((int)(vdata.Key.Hour / FileDuration)) * FileDuration, 0, 0);
                    dic.Add(new DataFileInfo6() { FileName = vfile, Duration = new TimeSpan(this.FileDuration, 0, 0), StartTime = vdd, IsZipFile = System.IO.Path.GetExtension(vfile) == DataFileManager.ZipDataFile2Extends }, new Tuple<DateTime, DateTime>(vdata.Key,vdata.Value));
                }
            }

            foreach (var vdata in dic)
            {
                vdata.Key.DeleteHisData<T>(id,vdata.Value.Item1,vdata.Value.Item2);
            }
        }

        ///// <summary>
        ///// 读取文件的第一个值
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="id"></param>
        ///// <param name="time"></param>
        ///// <param name="context"></param>
        ///// <returns></returns>
        //public object ReadFileFirstValue2<T>(int id,DateTime time,QueryContext context)
        //{
        //    if (context.IgnorCloseQuality)
        //    {
        //        var vtime = time;
        //        var fmanager = GetFileManager();
        //        while (true)
        //        {
        //            var vfile = fmanager.GetDataFileWithoutCheck(vtime, id);
        //            if (vfile != null && vfile is DataFileInfo6)
        //            {
        //                var val = (vfile as DataFileInfo6).ReadFileFirstAvaiableValue<T>(id, context);
        //                if(val==null)
        //                {
        //                    return (vfile as DataFileInfo6).ReadNextFileFirstAvaiableValue<T>(id, context);
        //                }
        //                return val;
        //            }
        //            vtime = vtime.AddHours(FileDuration);
        //            if ((vtime - time).TotalDays > 30 && vtime> fmanager.LastValueTime)
        //            {
        //                break;
        //            }
        //        }
        //    }
        //    else
        //    {
        //        var vfile = GetFileManager().GetDataFile(time, id);
        //        if (vfile != null && vfile is DataFileInfo6)
        //        {
        //            return (vfile as DataFileInfo6).ReadFileFirstAvaiableValue<T>(id, context);
        //        }
        //    }
        //    return null;
        //}


        /// <summary>
        /// 读取文件的第一个值,遇到无记录文件一直往后找
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public object ReadFileFirstValue<T>(int id, DateTime time, QueryContext context)
        {
            var vtime = time;
            var fmanager = GetFileManager();
            context.HasExitedQuality = false;
            while (true)
            {
                var vfile = fmanager.GetDataFileWithoutCheck(vtime, id);
                if (vfile != null && vfile is DataFileInfo6)
                {
                    var val = (vfile as DataFileInfo6).ReadFileFirstAvaiableValue<T>(id, context);
                    if (val == null)
                    {
                        if (!context.IgnorCloseQuality && context.HasExitedQuality)
                        {
                            break;
                        }
                        else
                        {
                            return (vfile as DataFileInfo6).ReadNextFileFirstAvaiableValue<T>(id, context);
                        }
                    }
                    return val;
                }
                vtime = vtime.AddHours(FileDuration);
                if ((vtime - time).TotalDays > 30 && vtime > fmanager.LastValueTime)
                {
                    break;
                }
            }
            return null;
        }

        ///// <summary>
        ///// 读取文件的最后一个记录值
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="id"></param>
        ///// <param name="time"></param>
        ///// <param name="context"></param>
        ///// <returns></returns>
        //public object ReadFileLastValue2<T>(int id, DateTime time, QueryContext context)
        //{
        //    if (context.IgnorCloseQuality)
        //    {
        //        var fmanager = GetFileManager();
        //        var vtime = time;
        //        while (true)
        //        {
        //            var vfile = fmanager.GetDataFileWithoutCheck(vtime, id);
        //            if (vfile != null && vfile is DataFileInfo6)
        //            {
        //                var val = (vfile as DataFileInfo6).ReadFileLastAvaiableValue<T>(id, context);
        //                if (val == null)
        //                {
        //                   return (vfile as DataFileInfo6).ReadPreFileLastAvaiableValue<T>(id, context);
        //                }
        //                return val;
        //            }
        //            else
        //            {
        //                vtime = vtime.AddHours(-FileDuration);
        //            }
        //            if(vtime< fmanager.FirstValueTime)
        //            {
        //                break;
        //            }
        //            //if ((time - vtime).TotalDays > 30)
        //            //{
        //            //    break;
        //            //}
        //        }
        //    }
        //    else
        //    {
        //        var vfile = GetFileManager().GetDataFile(time, id);
        //        if (vfile != null && vfile is DataFileInfo6)
        //        {
        //            return (vfile as DataFileInfo6).ReadFileLastAvaiableValue<T>(id, context);
        //        }
        //    }
        //    return null;
        //}

        /// <summary>
        /// 读取文件的最后一个记录值,遇到无记录文件一直往前找
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public object ReadFileLastValue<T>(int id, DateTime time, QueryContext context)
        {
            var fmanager = GetFileManager();
            var vtime = time;
            context.HasExitedQuality = false;
            while (true)
            {
                var vfile = fmanager.GetDataFileWithoutCheck(vtime, id);
                if (vfile != null && vfile is DataFileInfo6)
                {
                    var val = (vfile as DataFileInfo6).ReadFileLastAvaiableValue<T>(id, context);
                    if (val == null)
                    {
                        if (!context.IgnorCloseQuality && context.HasExitedQuality)
                        {
                            break;
                        }
                        else
                        {
                            return (vfile as DataFileInfo6).ReadPreFileLastAvaiableValue<T>(id, context);
                        }
                    }
                    return val;
                }
                else
                {
                    if(vtime>DateTime.MinValue)
                    vtime = vtime.AddHours(-FileDuration);
                }
                if (vtime < fmanager.FirstValueTime)
                {
                    break;
                }
            }

            return null;
        }

        #endregion

        /// <summary>
        /// 读取变量最后更新的时间
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public DateTime ReadTagLastAvaiableValueTime<T>(int id)
        {
            var fileMananger = GetFileManager();
            if(fileMananger.LastValueTime == DateTime.MinValue) 
                return DateTime.MinValue;

            var stime = fileMananger.LastValueTime.AddDays(-5);
            var etime = new TimeSpan(5,0, 0, 0);
            while(true)
            {
                var vfile = fileMananger.GetDataFiles(stime, etime, id);
                if(vfile!=null && vfile.Count> 0)
                {
                    vfile.Reverse();
                    foreach (var vv in vfile)
                    {
                        var obj = (vv as DataFileInfo6).ReadFileLastAvaiableValue<T>(id, new QueryContext() { IgnorCloseQuality = true });
                        if (obj != null)
                        {
                            return ((TagHisValue<T>)obj).Time;
                        }
                    }
                }
                stime = stime.AddDays(-5);
                if(stime<fileMananger.FirstValueTime)
                {
                    break;
                }
            }
           
            return DateTime.MinValue;
        }

        public void Dispose()
        {
            mMemoryService=null;
            statisticsHelper?.Dispose();
            statisticsHelper = null;
        }
    }
}
