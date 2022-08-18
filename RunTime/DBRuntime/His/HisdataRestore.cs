using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBRuntime.His
{
    /// <summary>
    /// 历史数据缓存恢复管理
    /// </summary>
    public class HisDataRestore
    {
        public static HisDataRestore Instance = new HisDataRestore();

        /// <summary>
        /// 检查并恢复异常数据
        /// </summary>
        public void CheckAndRestore()
        {
            var rgs = LogStorageManager.Instance.ListAllLogRegion();
            if(rgs.Count > 0)
            {
                int i = 1;
                int count = rgs.Count;
                foreach(var rr in rgs)
                {
                    LoggerService.Service.Info("HisDataRestore", $"开始恢复因非正常退出,未存盘的数据....{i}/{count}");
                    ProcessLogRegion(rr.Value);
                    rr.Value.Dispose();
                    i++;
                    rr.Value.Clear();
                }
            }

            ServiceLocator.Locator.Resolve<IHisDataManagerService>().ClearTmpFile();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="log"></param>
        private void ProcessLogRegion(LogRegion log)
        {
            if (log != null)
            {
                log.Open();
                Dictionary<int, SortedDictionary<DateTime, CachItem>> mValues = new Dictionary<int, SortedDictionary<DateTime, CachItem>>();
                foreach (var vv in log.ListAllRead())
                {
                    if (mValues.ContainsKey(vv.Id))
                    {
                        if (!mValues[vv.Id].ContainsKey(vv.Time))
                        {
                            mValues[vv.Id].Add(vv.Time, vv);
                        }
                        else
                        {
                            mValues[vv.Id][vv.Time] = vv;
                            LoggerService.Service.Warn("HisDataRestore", $"{vv.Id} {vv.Time} {vv.Value} 冲突!");
                        }
                    }
                    else
                    {
                        var sd = new SortedDictionary<DateTime, CachItem>();
                        sd.Add(vv.Time, vv);
                        mValues.Add(vv.Id, sd);
                    }
                }

                //追加结束标识
                foreach (var vv in mValues.Values)
                {
                    if (vv.Values.Count > 0)
                    {
                        var last = vv.Values.Last();
                        vv.Add(last.Time.AddMilliseconds(10), new CachItem() { Time = last.Time.AddMilliseconds(10), Id = last.Id, Quality = (byte)QualityConst.Close, Type = last.Type, Value = last.Value });
                    }
                }

                int i = 0;
                int count = mValues.Count;


                int mlastpos = Console.GetCursorPosition().Top;

                string spath = System.IO.Path.GetFileNameWithoutExtension(log.FileName);

                //执行数据写入
                //读取信息
                MarshalMemoryBlock mmb = null;
                foreach (var vv in mValues)
                {
                    i++;

                    if (i % 100 == 0)
                    {
                        int cc = Console.GetCursorPosition().Top;
                        Console.SetCursorPosition(0, mlastpos);
                        LoggerService.Service.Info("HisDataRestore", $"{ spath } 已恢复变量个数 {i} / {count} ");
                        if (cc != mlastpos)
                            Console.SetCursorPosition(0, cc);
                    }

                    var vtyp = vv.Value.First().Value.Type;
                    var id = vv.Key;
                    var time = vv.Value.First().Key;
                    switch (vtyp)
                    {
                        case TagType.Bool:
                            HisQueryResult<bool> bhr = new HisQueryResult<bool>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                bhr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<bool>(id, time, bhr);
                            bhr.Dispose();
                            break;
                        case TagType.Byte:
                            HisQueryResult<byte> bbhr = new HisQueryResult<byte>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                bbhr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<byte>(id, time, bbhr);
                            bbhr.Dispose();
                            break;
                        case TagType.Short:
                            HisQueryResult<short> shr = new HisQueryResult<short>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                shr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<short>(id, time, shr);
                            shr.Dispose();
                            break;
                        case TagType.UShort:
                            HisQueryResult<ushort> ushr = new HisQueryResult<ushort>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                ushr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<ushort>(id, time, ushr);
                            ushr.Dispose();
                            break;
                        case TagType.Int:
                            HisQueryResult<int> ishr = new HisQueryResult<int>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                ishr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<int>(id, time, ishr);
                            ishr.Dispose();
                            break;
                        case TagType.UInt:
                            HisQueryResult<uint> uishr = new HisQueryResult<uint>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                uishr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<uint>(id, time, uishr);
                            uishr.Dispose();
                            break;
                        case TagType.Long:
                            HisQueryResult<long> lshr = new HisQueryResult<long>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                lshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<long>(id, time, lshr);
                            lshr.Dispose();
                            break;
                        case TagType.ULong:
                            HisQueryResult<ulong> ulshr = new HisQueryResult<ulong>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                ulshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<ulong>(id, time, ulshr);
                            ulshr.Dispose();
                            break;
                        case TagType.Double:
                            HisQueryResult<double> dshr = new HisQueryResult<double>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                dshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<double>(id, time, dshr);
                            dshr.Dispose();
                            break;
                        case TagType.Float:
                            HisQueryResult<float> fshr = new HisQueryResult<float>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                fshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<float>(id, time, fshr);
                            fshr.Dispose();
                            break;
                        case TagType.String:
                            HisQueryResult<string> sfshr = new HisQueryResult<string>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                sfshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<string>(id, time, sfshr);
                            sfshr.Dispose();
                            break;
                        case TagType.DateTime:
                            HisQueryResult<DateTime> dtshr = new HisQueryResult<DateTime>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                dtshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<DateTime>(id, time, dtshr);
                            dtshr.Dispose();
                            break;
                        case TagType.IntPoint:
                            HisQueryResult<IntPointData> ptshr = new HisQueryResult<IntPointData>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                ptshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<IntPointData>(id, time, ptshr);
                            ptshr.Dispose();
                            break;
                        case TagType.UIntPoint:
                            HisQueryResult<UIntPointData> uptshr = new HisQueryResult<UIntPointData>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                uptshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<UIntPointData>(id, time, uptshr);
                            uptshr.Dispose();
                            break;
                        case TagType.IntPoint3:
                            HisQueryResult<IntPoint3Data> p3tshr = new HisQueryResult<IntPoint3Data>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                p3tshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<IntPoint3Data>(id, time, p3tshr);
                            p3tshr.Dispose();
                            break;
                        case TagType.UIntPoint3:
                            HisQueryResult<UIntPoint3Data> up3tshr = new HisQueryResult<UIntPoint3Data>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                up3tshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<UIntPoint3Data>(id, time, up3tshr);
                            up3tshr.Dispose();
                            break;
                        case TagType.LongPoint:
                            HisQueryResult<LongPointData> lptshr = new HisQueryResult<LongPointData>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                lptshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<LongPointData>(id, time, lptshr);
                            lptshr.Dispose();
                            break;
                        case TagType.ULongPoint:
                            HisQueryResult<ULongPointData> ulptshr = new HisQueryResult<ULongPointData>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                ulptshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<ULongPointData>(id, time, ulptshr);
                            ulptshr.Dispose();
                            break;
                        case TagType.LongPoint3:
                            HisQueryResult<LongPoint3Data> lp3tshr = new HisQueryResult<LongPoint3Data>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                lp3tshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<LongPoint3Data>(id, time, lp3tshr);
                            lp3tshr.Dispose();
                            break;
                        case TagType.ULongPoint3:
                            HisQueryResult<ULongPoint3Data> ulp3tshr = new HisQueryResult<ULongPoint3Data>(vv.Value.Values.Count);
                            foreach (var v in vv.Value.Values)
                            {
                                ulp3tshr.Add(v.Value, v.Time, v.Quality);
                            }
                            mmb = Compile<ULongPoint3Data>(id, time, ulp3tshr);
                            ulp3tshr.Dispose();
                            break;

                    }

                    //写入磁盘
                    if (mmb != null)
                    {
                        ServiceLocator.Locator.Resolve<IHisDataManagerService>().SaveData(id, time, mmb, SaveType.Append,"");
                        ServiceLocator.Locator.Resolve<IDataCompressService>().Release(mmb);
                    }
                }
                if(i%100!=0)
                {
                    int cc = Console.GetCursorPosition().Top;
                    Console.SetCursorPosition(0, mlastpos);
                    LoggerService.Service.Info("HisDataRestore", $"{ spath } 已恢复变量个数 {i} / {count} ");
                    if (cc != mlastpos)
                        Console.SetCursorPosition(0, cc);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="datas"></param>
        /// <returns></returns>
        private MarshalMemoryBlock Compile<T>(int id,DateTime time,HisQueryResult<T> datas)
        {
            return ServiceLocator.Locator.Resolve<IDataCompressService>().CompressData(id,time,datas);
        }
    }
}
