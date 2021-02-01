//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/4 16:40:03.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 从日志文件种读取
    /// </summary>
    public class LogFileInfo
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        
        /// <summary>
        /// 
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string FileName { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {

        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public static class LogFileInfoExtend
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static DataFileSeriserbase GetFileSeriser(this LogFileInfo file)
        {
            var re = DataFileSeriserManager.manager.GetDefaultFileSersie();
            re.FileName = file.FileName;
            re.OpenForReadOnly(file.FileName);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue<T>(this LogFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            file.GetFileSeriser().ReadAllValue(tid,startTime,endTime,file.StartTime,result);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void Read<T>(this LogFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            file.GetFileSeriser().Read(tid, times, type, file.StartTime, result);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="tid"></param>
        /// <returns></returns>
        private static AddressAndSize ReadTagIndex(this DataFileSeriserbase datafile,int tid,out long offset,out short len)
        {
            len = datafile.ReadShort(0); //读取时长
            var vsize = datafile.ReadInt(10);
            offset = vsize+6;

            lock (TagHeadOffsetManager.manager.LogHeadOffsets)
            {
                if (!TagHeadOffsetManager.manager.Contains(datafile.FileName))
                {
                    Dictionary<int, AddressAndSize> dtmps = new Dictionary<int, AddressAndSize>();
                    var idandaddress = datafile.Read(14, vsize);
                    int id = idandaddress.ReadInt(0);
                    long address = idandaddress.ReadLong(4);
                    int size = 0;
                    long ltmp;
                    int nid = 0;
                    for (int i=1;i<vsize/12;i++)
                    {
                        nid = idandaddress.ReadInt(i * 12);
                        ltmp = idandaddress.ReadLong(i * 12+4);
                        size = (int)(ltmp - address);
                        dtmps.Add(id, new AddressAndSize() { Address = address, Size = size });
                        address = ltmp;
                        id = nid;
                    }

                    dtmps.Add(id, new AddressAndSize() { Address = address, Size = (int)(datafile.Length - address) });

                    TagHeadOffsetManager.manager.AddLogHead(datafile.FileName, dtmps);
                        if (dtmps.ContainsKey(tid))
                            return dtmps[tid];
                }
                else
                {
                   var idaddrs = TagHeadOffsetManager.manager.GetLog(datafile.FileName);
                    if (idaddrs.ContainsKey(tid))
                        return idaddrs[tid];
                }
            }
            return AddressAndSize.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="timeraddr"></param>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <returns></returns>
        private static Dictionary<int,DateTime> ReadTimeIndex(this DataFileSeriserbase datafile,long offset,long timeraddr,DateTime starttime,DateTime endtime,DateTime time,int count)
        {
            Dictionary<int, DateTime> re = new Dictionary<int, DateTime>();
            var basetime = DateTime.FromBinary(datafile.ReadLong(2));
            for (int i = 0; i < count; i++)
            {
                var vf = basetime.AddMilliseconds(datafile.ReadShort(offset + timeraddr + i * 2) * 100);
                if (vf >= starttime && vf < endtime)
                {
                    if(vf!=time|| i==0)
                    re.Add(i,vf);
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="timeraddr"></param>
        /// <param name="time"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        private static Dictionary<int,Tuple<DateTime,bool>> ReadTimeIndex(this DataFileSeriserbase datafile, long offset, long timeraddr,  DateTime time, int count)
        {
            Dictionary<int, Tuple<DateTime, bool>> re = new Dictionary<int, Tuple<DateTime, bool>>();
            for (int i = 0; i < count; i++)
            {
                var vf = time.AddMilliseconds(datafile.ReadShort(offset + timeraddr + i * 2) * 100);
                if (vf != time || i == 0)
                {
                    re.Add(i, new Tuple<DateTime, bool>(vf, true));
                }
                else
                {
                    re.Add(i, new Tuple<DateTime, bool>(vf, false));
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="valIndex"></param>
        /// <param name="offset"></param>
        /// <param name="valueaddr"></param>
        /// <param name="result"></param>
        private static List<object> ReadValueInner<T>(this DataFileSeriserbase datafile,List<int> valIndex,long offset,long valueaddr,out int datasize)
        {
            List<object> re = new List<object>();
            if (typeof(T) == typeof(bool))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(Convert.ToBoolean(datafile.ReadByte(offset + valueaddr + vv)));
                }
                datasize = 1;
            }
            else if (typeof(T) == typeof(byte))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadByte(offset + valueaddr + vv));
                }
                datasize = 1;
            }
            else if (typeof(T) == typeof(short))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadShort(offset + valueaddr + vv * 2));
                }
                datasize = 2;
            }
            else if (typeof(T) == typeof(ushort))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((ushort)datafile.ReadShort(offset + valueaddr + vv * 2));
                }
                datasize = 2;
            }
            else if (typeof(T) == typeof(int))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadInt(offset + valueaddr + vv * 4));
                }
                datasize = 4;
            }
            else if (typeof(T) == typeof(uint))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((uint)datafile.ReadInt(offset + valueaddr + vv * 4));
                }
                datasize = 4;
            }
            else if (typeof(T) == typeof(long))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((long)datafile.ReadLong(offset + valueaddr + vv * 8));
                }
                datasize = 8;
            }
            else if (typeof(T) == typeof(ulong))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((ulong)datafile.ReadLong(offset + valueaddr + vv * 8));
                }
                datasize = 8;
            }
            else if (typeof(T) == typeof(double))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadDouble(offset + valueaddr + vv * 8));
                }
                datasize = 8;
            }
            else if (typeof(T) == typeof(float))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadFloat(offset + valueaddr + vv * 4));
                }
                datasize = 4;
            }
            else if (typeof(T) == typeof(string))
            {
                foreach (var vv in valIndex)
                {
                    var str = Encoding.Unicode.GetString(datafile.ReadBytes(offset + valueaddr + vv * Const.StringSize, Const.StringSize));
                    re.Add(str);
                }
                datasize = Const.StringSize;
            }
            else if (typeof(T) == typeof(DateTime))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadDateTime(offset + valueaddr + vv * 8));
                }
                datasize = 8;
            }
            else if (typeof(T) == typeof(IntPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = datafile.ReadInt(offset + valueaddr + vv * 8);
                    var y = datafile.ReadInt(offset + valueaddr + vv * 8 + 4);
                    re.Add(new IntPointData() { X = x, Y = y });
                }
                datasize = 8;
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = (uint)datafile.ReadInt(offset + valueaddr + vv * 8);
                    var y = (uint)datafile.ReadInt(offset + valueaddr + vv * 8 + 4);
                    re.Add(new UIntPointData() { X = x, Y = y });
                }
                datasize = 8;
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = (long)datafile.ReadLong(offset + valueaddr + vv * 16);
                    var y = (long)datafile.ReadLong(offset + valueaddr + vv * 16 + 8);
                    re.Add(new LongPointData() { X = x, Y = y });
                }
                datasize = 16;
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = (ulong)datafile.ReadLong(offset + valueaddr + vv * 16);
                    var y = (ulong)datafile.ReadLong(offset + valueaddr + vv * 16 + 8);
                    re.Add(new ULongPointData() { X = x, Y = y });
                }
                datasize = 16;
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                foreach (var vv in valIndex)
                {
                    var x = datafile.ReadInt(offset + valueaddr + vv * 12);
                    var y = datafile.ReadInt(offset + valueaddr + vv * 12 + 4);
                    var z = datafile.ReadInt(offset + valueaddr + vv * 12 + 8);
                    re.Add(new IntPoint3Data() { X = x, Y = y, Z = z });
                }
                datasize = 12;
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                foreach (var vv in valIndex)
                {
                    var x = (uint)datafile.ReadInt(offset + valueaddr + vv * 12);
                    var y = (uint)datafile.ReadInt(offset + valueaddr + vv * 12 + 4);
                    var z = (uint)datafile.ReadInt(offset + valueaddr + vv * 12 + 8);
                    re.Add(new UIntPoint3Data() { X = x, Y = y, Z = z });
                }
                datasize = 12;
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                foreach (var vv in valIndex)
                {
                    var x = (long)datafile.ReadLong(offset + valueaddr + vv * 24);
                    var y = (long)datafile.ReadLong(offset + valueaddr + vv * 24 + 8);
                    var z = (long)datafile.ReadLong(offset + valueaddr + vv * 24 + 168);
                    re.Add(new LongPoint3Data() { X = x, Y = y, Z = z });
                }
                datasize = 24;
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                foreach (var vv in valIndex)
                {
                    var x = (ulong)datafile.ReadLong(offset + valueaddr + vv * 24);
                    var y = (ulong)datafile.ReadLong(offset + valueaddr + vv * 24 + 8);
                    var z = (ulong)datafile.ReadLong(offset + valueaddr + vv * 24 + 168);
                    re.Add(new ULongPoint3Data() { X = x, Y = y, Z = z });
                }
                datasize = 24;
            }
            else
            {
                datasize = 0;
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="time"></param>
        /// <param name="result"></param>
        public static void ReadAllValue<T>(this DataFileSeriserbase datafile, int tid, DateTime startTime, DateTime endTime,DateTime time, HisQueryResult<T> result)
        {
            long addroffset = 0;
            short len = 0;
            int datasize = 0;

            if (!datafile.IsOpened()) return;

            var aid = datafile.ReadTagIndex(tid,out addroffset,out len);
            if(!aid.IsEmpty())
            {
                int tagcount = len * 60;
                var tindex = ReadTimeIndex(datafile, 0, aid.Address, startTime, endTime,time, tagcount);
                var vals = ReadValueInner<T>(datafile,tindex.Keys.ToList(), 0, aid.Address + tagcount * 2,out datasize);
                var qus = datafile.ReadBytes(aid.Address + 0 + tagcount * (2 + datasize), tagcount);
                int i = 0;
                foreach(var vv in tindex)
                {
                    result.Add(vals[i], vv.Value, qus[vv.Key]);
                    i++;
                }
            }
        }

        private static int GetValueSize<T>()
        {
            if (typeof(T) == typeof(bool))
            {
                return 1;
            }
            else if (typeof(T) == typeof(byte))
            {
                return 1;
            }
            else if (typeof(T) == typeof(short))
            {
                return 2;
            }
            else if (typeof(T) == typeof(ushort))
            {
                return 2;
            }
            else if (typeof(T) == typeof(int))
            {
                return 4;
            }
            else if (typeof(T) == typeof(uint))
            {
                return 4;
            }
            else if (typeof(T) == typeof(long))
            {
                return 8;
            }
            else if (typeof(T) == typeof(ulong))
            {
                return 8;
            }
            else if (typeof(T) == typeof(double))
            {
                return 8;
            }
            else if (typeof(T) == typeof(float))
            {
                return 4;
            }
            else if (typeof(T) == typeof(string))
            {
                return Const.StringSize;
            }
            else if (typeof(T) == typeof(DateTime))
            {
                return 8;

            }
            else if (typeof(T) == typeof(IntPointData))
            {
                return 8;
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                return 8;
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                return 16;
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                return 16;
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                
                return 12;
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                return 12;
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                return 24;
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                return 24;
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="time"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <returns></returns>
        private static object LinerValue<T>(DateTime startTime, DateTime endTime, DateTime time, T value1, T value2)
        {
            var pval1 = (time - startTime).TotalMilliseconds;
            var tval1 = (endTime - startTime).TotalMilliseconds;

            if (typeof(T) == typeof(IntPointData))
            {
                var sval1 = (IntPointData)((object)value1);
                var sval2 = (IntPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new IntPointData((int)val1, (int)val2);
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                var sval1 = (UIntPointData)((object)value1);
                var sval2 = (UIntPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new UIntPointData((uint)val1, (uint)val2);
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                var sval1 = (LongPointData)((object)value1);
                var sval2 = (LongPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new LongPointData((long)val1, (long)val2);
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                var sval1 = (ULongPointData)((object)value1);
                var sval2 = (ULongPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new ULongPointData((ulong)val1, (ulong)val2);
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                var sval1 = (IntPoint3Data)((object)value1);
                var sval2 = (IntPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new IntPoint3Data((int)val1, (int)val2, (int)val3);
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                var sval1 = (UIntPoint3Data)((object)value1);
                var sval2 = (UIntPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new UIntPoint3Data((uint)val1, (uint)val2, (uint)val3);
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                var sval1 = (LongPoint3Data)((object)value1);
                var sval2 = (LongPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new LongPoint3Data((long)val1, (long)val2, (long)val3);
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                var sval1 = (ULongPoint3Data)((object)value1);
                var sval2 = (ULongPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new ULongPoint3Data((ulong)val1, (ulong)val2, (ulong)val3);
            }

            return default(T);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private static bool CheckTypeIsPointData(Type type)
        {
            return type == typeof(IntPointData) || type == typeof(UIntPointData) || type == typeof(LongPointData) || type == typeof(ULongPointData) || type == typeof(IntPoint3Data) || type == typeof(UIntPoint3Data) || type == typeof(LongPoint3Data) || type == typeof(ULongPoint3Data);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="startTime"></param>
        /// <param name="result"></param>
        public static void Read<T>(this DataFileSeriserbase datafile, int tid, List<DateTime> times, QueryValueMatchType type, DateTime startTime,HisQueryResult<T> result)
        {
            long addroffset = 0;
            short len = 0;
            int datasize = 0;
            if (!datafile.IsOpened()) return;

            var aid = datafile.ReadTagIndex(tid, out addroffset, out len);
            if(!aid.IsEmpty())
            {
                int tagcount = len * 60;
                var qs = ReadTimeIndex(datafile, 0, aid.Address, startTime, tagcount);
                
                var vals = ReadValueInner<T>(datafile, qs.Keys.ToList(), 0, aid.Address + tagcount * 2, out datasize);
                var qq = datafile.ReadBytes(aid.Address + 0 + tagcount * (datasize + 2), tagcount);

                var vv = qs.ToArray();
                //long valaddr = addroffset + tagcount * 2;
                int count = 0;
                foreach (var time1 in times)
                {
                    for (int i = 0; i < vv.Length - 1; i++)
                    {
                        var skey = vv[i];

                        var snext = vv[i + 1];

                        if (time1 == skey.Value.Item1)
                        {
                            result.Add(vals[i], time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                        else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                        {

                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    result.Add(vals[i], time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.After:
                                    result.Add(vals[i+1], time1, qq[snext.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.Linear:
                                    if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                                    {
                                        var ppval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                        var ffval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                        if (ppval < ffval)
                                        {
                                            result.Add(vals[i], time1, qq[skey.Key]);
                                        }
                                        else
                                        {
                                            result.Add(vals[i + 1], time1, qq[snext.Key]);
                                        }
                                        count++;
                                    }
                                    else
                                    {
                                       
                                        if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                        {
                                            if (CheckTypeIsPointData(typeof(T)))
                                            {
                                                result.Add(LinerValue(skey.Value.Item1, snext.Value.Item1, time1, vals[i], vals[i + 1]), time1, 0);
                                            }
                                            else
                                            {
                                                var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                                var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                                var sval1 = (double)vals[i];
                                                var sval2 = (double)vals[i + 1];
                                                var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                                result.Add((object)val1, time1, 0);
                                            }
                                        }
                                        else if (qq[skey.Key] < 20)
                                        {
                                            result.Add(vals[i], time1, qq[skey.Key]);
                                        }
                                        else if (qq[snext.Key] < 20)
                                        {
                                            result.Add(vals[i + 1], time1, qq[snext.Key]);
                                        }
                                        else
                                        {
                                            result.Add(default(T), time1, (byte)QualityConst.Null);
                                        }
                                    }
                                    count++;
                                    break;
                                case QueryValueMatchType.Closed:
                                    var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(vals[i], time1, qq[skey.Key]);
                                    }
                                    else
                                    {
                                        result.Add(vals[i + 1], time1, qq[snext.Key]);
                                    }
                                    count++;
                                    break;
                            }

                            break;
                        }
                        else if (time1 == snext.Value.Item1)
                        {
                            result.Add(vals[i + 1], time1, qq[snext.Key]);
                            count++;
                            break;
                        }

                    }
                }
            }
        }
    }

}
