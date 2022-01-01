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

            if (vsize > 7)
            {
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
                        for (int i = 1; i < vsize / 12; i++)
                        {
                            nid = idandaddress.ReadInt(i * 12);
                            ltmp = idandaddress.ReadLong(i * 12 + 4);
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

            string tname = typeof(T).Name;
            switch (tname)
            {
                case "Boolean":
                    foreach (var vv in valIndex)
                    {
                        re.Add(Convert.ToBoolean(datafile.ReadByte(offset + valueaddr + vv)));
                    }
                    datasize = 1;
                    break;
                case "Byte":
                    foreach (var vv in valIndex)
                    {
                        re.Add(datafile.ReadByte(offset + valueaddr + vv));
                    }
                    datasize = 1;
                    break;
                case "Int16":
                    foreach (var vv in valIndex)
                    {
                        re.Add(datafile.ReadShort(offset + valueaddr + vv * 2));
                    }
                    datasize = 2;
                    break;
                case "UInt16":
                    foreach (var vv in valIndex)
                    {
                        re.Add((ushort)datafile.ReadShort(offset + valueaddr + vv * 2));
                    }
                    datasize = 2;
                    break;
                case "Int32":
                    foreach (var vv in valIndex)
                    {
                        re.Add(datafile.ReadInt(offset + valueaddr + vv * 4));
                    }
                    datasize = 4;
                    break;
                case "UInt32":
                    foreach (var vv in valIndex)
                    {
                        re.Add((uint)datafile.ReadInt(offset + valueaddr + vv * 4));
                    }
                    datasize = 4;
                    break;
                case "Int64":
                    foreach (var vv in valIndex)
                    {
                        re.Add((long)datafile.ReadLong(offset + valueaddr + vv * 8));
                    }
                    datasize = 8;
                    break;
                case "UInt64":
                    foreach (var vv in valIndex)
                    {
                        re.Add((ulong)datafile.ReadLong(offset + valueaddr + vv * 8));
                    }
                    datasize = 8;
                    break;
                case "Double":
                    foreach (var vv in valIndex)
                    {
                        re.Add(datafile.ReadDouble(offset + valueaddr + vv * 8));
                    }
                    datasize = 8;
                    break;
                case "Single":
                    foreach (var vv in valIndex)
                    {
                        re.Add(datafile.ReadFloat(offset + valueaddr + vv * 4));
                    }
                    datasize = 4;
                    break;
                case "String":
                    foreach (var vv in valIndex)
                    {
                        var str = Encoding.Unicode.GetString(datafile.ReadBytes(offset + valueaddr + vv * Const.StringSize, Const.StringSize));
                        re.Add(str);
                    }
                    datasize = Const.StringSize;
                    break;
                case "DateTime":
                    foreach (var vv in valIndex)
                    {
                        re.Add(datafile.ReadDateTime(offset + valueaddr + vv * 8));
                    }
                    datasize = 8;
                    break;
                case "IntPointData":
                    foreach (var vv in valIndex)
                    {
                        var x = datafile.ReadInt(offset + valueaddr + vv * 8);
                        var y = datafile.ReadInt(offset + valueaddr + vv * 8 + 4);
                        re.Add(new IntPointData() { X = x, Y = y });
                    }
                    datasize = 8;
                    break;
                case "UIntPointData":
                    foreach (var vv in valIndex)
                    {
                        var x = (uint)datafile.ReadInt(offset + valueaddr + vv * 8);
                        var y = (uint)datafile.ReadInt(offset + valueaddr + vv * 8 + 4);
                        re.Add(new UIntPointData() { X = x, Y = y });
                    }
                    datasize = 8;
                    break;
                case "LongPointData":
                    foreach (var vv in valIndex)
                    {
                        var x = (long)datafile.ReadLong(offset + valueaddr + vv * 16);
                        var y = (long)datafile.ReadLong(offset + valueaddr + vv * 16 + 8);
                        re.Add(new LongPointData() { X = x, Y = y });
                    }
                    datasize = 16;
                    break;
                case "ULongPointData":
                    foreach (var vv in valIndex)
                    {
                        var x = (ulong)datafile.ReadLong(offset + valueaddr + vv * 16);
                        var y = (ulong)datafile.ReadLong(offset + valueaddr + vv * 16 + 8);
                        re.Add(new ULongPointData() { X = x, Y = y });
                    }
                    datasize = 16;
                    break;
                case "IntPoint3Data":
                    foreach (var vv in valIndex)
                    {
                        var x = datafile.ReadInt(offset + valueaddr + vv * 12);
                        var y = datafile.ReadInt(offset + valueaddr + vv * 12 + 4);
                        var z = datafile.ReadInt(offset + valueaddr + vv * 12 + 8);
                        re.Add(new IntPoint3Data() { X = x, Y = y, Z = z });
                    }
                    datasize = 12;
                    break;
                case "UIntPoint3Data":
                    foreach (var vv in valIndex)
                    {
                        var x = (uint)datafile.ReadInt(offset + valueaddr + vv * 12);
                        var y = (uint)datafile.ReadInt(offset + valueaddr + vv * 12 + 4);
                        var z = (uint)datafile.ReadInt(offset + valueaddr + vv * 12 + 8);
                        re.Add(new UIntPoint3Data() { X = x, Y = y, Z = z });
                    }
                    datasize = 12;
                    break;
                case "LongPoint3Data":
                    foreach (var vv in valIndex)
                    {
                        var x = (long)datafile.ReadLong(offset + valueaddr + vv * 24);
                        var y = (long)datafile.ReadLong(offset + valueaddr + vv * 24 + 8);
                        var z = (long)datafile.ReadLong(offset + valueaddr + vv * 24 + 168);
                        re.Add(new LongPoint3Data() { X = x, Y = y, Z = z });
                    }
                    datasize = 24;
                    break;
                case "ULongPoint3Data":
                    foreach (var vv in valIndex)
                    {
                        var x = (ulong)datafile.ReadLong(offset + valueaddr + vv * 24);
                        var y = (ulong)datafile.ReadLong(offset + valueaddr + vv * 24 + 8);
                        var z = (ulong)datafile.ReadLong(offset + valueaddr + vv * 24 + 168);
                        re.Add(new ULongPoint3Data() { X = x, Y = y, Z = z });
                    }
                    datasize = 24;
                    break;
                default:
                    datasize = 0;
                    break;
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
                var pretime = DateTime.MinValue;
                foreach(var vv in tindex)
                {
                    if (qus[vv.Key] < 100 && vv.Value > pretime)
                    {
                        pretime = vv.Value;
                        result.Add(vals[i], vv.Value, qus[vv.Key]);
                        i++;
                    }
                }
            }
        }

        //private static int GetValueSize<T>()
        //{
        //    if (typeof(T) == typeof(bool))
        //    {
        //        return 1;
        //    }
        //    else if (typeof(T) == typeof(byte))
        //    {
        //        return 1;
        //    }
        //    else if (typeof(T) == typeof(short))
        //    {
        //        return 2;
        //    }
        //    else if (typeof(T) == typeof(ushort))
        //    {
        //        return 2;
        //    }
        //    else if (typeof(T) == typeof(int))
        //    {
        //        return 4;
        //    }
        //    else if (typeof(T) == typeof(uint))
        //    {
        //        return 4;
        //    }
        //    else if (typeof(T) == typeof(long))
        //    {
        //        return 8;
        //    }
        //    else if (typeof(T) == typeof(ulong))
        //    {
        //        return 8;
        //    }
        //    else if (typeof(T) == typeof(double))
        //    {
        //        return 8;
        //    }
        //    else if (typeof(T) == typeof(float))
        //    {
        //        return 4;
        //    }
        //    else if (typeof(T) == typeof(string))
        //    {
        //        return Const.StringSize;
        //    }
        //    else if (typeof(T) == typeof(DateTime))
        //    {
        //        return 8;

        //    }
        //    else if (typeof(T) == typeof(IntPointData))
        //    {
        //        return 8;
        //    }
        //    else if (typeof(T) == typeof(UIntPointData))
        //    {
        //        return 8;
        //    }
        //    else if (typeof(T) == typeof(LongPointData))
        //    {
        //        return 16;
        //    }
        //    else if (typeof(T) == typeof(ULongPointData))
        //    {
        //        return 16;
        //    }
        //    else if (typeof(T) == typeof(IntPoint3Data))
        //    {
                
        //        return 12;
        //    }
        //    else if (typeof(T) == typeof(UIntPoint3Data))
        //    {
        //        return 12;
        //    }
        //    else if (typeof(T) == typeof(LongPoint3Data))
        //    {
        //        return 24;
        //    }
        //    else if (typeof(T) == typeof(ULongPoint3Data))
        //    {
        //        return 24;
        //    }
        //    return 0;
        //}

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

            string tname = typeof(T).Name;
            switch (tname)
            {
                case "IntPointData":
                    var sval1 = (IntPointData)((object)value1);
                    var sval2 = (IntPointData)((object)value2);
                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                    var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                    return new IntPointData((int)val1, (int)val2);
                case "UIntPointData":
                    var usval1 = (UIntPointData)((object)value1);
                    var usval2 = (UIntPointData)((object)value2);
                    var uval1 = pval1 / tval1 * (Convert.ToDouble(usval2.X) - Convert.ToDouble(usval1.X)) + Convert.ToDouble(usval1.X);
                    var uval2 = pval1 / tval1 * (Convert.ToDouble(usval2.Y) - Convert.ToDouble(usval1.Y)) + Convert.ToDouble(usval1.Y);
                    return new UIntPointData((uint)uval1, (uint)uval2);
                case "LongPointData":
                    var lsval1 = (LongPointData)((object)value1);
                    var lsval2 = (LongPointData)((object)value2);
                    var lval1 = pval1 / tval1 * (Convert.ToDouble(lsval2.X) - Convert.ToDouble(lsval1.X)) + Convert.ToDouble(lsval1.X);
                    var lval2 = pval1 / tval1 * (Convert.ToDouble(lsval2.Y) - Convert.ToDouble(lsval1.Y)) + Convert.ToDouble(lsval1.Y);
                    return new LongPointData((long)lval1, (long)lval2);
                case "ULongPointData":
                    var ulsval1 = (ULongPointData)((object)value1);
                    var ulsval2 = (ULongPointData)((object)value2);
                    var ulval1 = pval1 / tval1 * (Convert.ToDouble(ulsval2.X) - Convert.ToDouble(ulsval1.X)) + Convert.ToDouble(ulsval1.X);
                    var ulval2 = pval1 / tval1 * (Convert.ToDouble(ulsval2.Y) - Convert.ToDouble(ulsval1.Y)) + Convert.ToDouble(ulsval1.Y);
                    return new ULongPointData((ulong)ulval1, (ulong)ulval2);
                case "IntPoint3Data":
                    var s3val1 = (IntPoint3Data)((object)value1);
                    var s3val2 = (IntPoint3Data)((object)value2);
                    var v3al1 = pval1 / tval1 * (Convert.ToDouble(s3val2.X) - Convert.ToDouble(s3val1.X)) + Convert.ToDouble(s3val1.X);
                    var v3al2 = pval1 / tval1 * (Convert.ToDouble(s3val2.Y) - Convert.ToDouble(s3val1.Y)) + Convert.ToDouble(s3val1.Y);
                    var v3al3 = pval1 / tval1 * (Convert.ToDouble(s3val2.Z) - Convert.ToDouble(s3val1.Z)) + Convert.ToDouble(s3val1.Z);
                    return new IntPoint3Data((int)v3al1, (int)v3al2, (int)v3al3);
                case "UIntPoint3Data":
                    var us3val1 = (UIntPoint3Data)((object)value1);
                    var us3val2 = (UIntPoint3Data)((object)value2);
                    var uv3al1 = pval1 / tval1 * (Convert.ToDouble(us3val2.X) - Convert.ToDouble(us3val1.X)) + Convert.ToDouble(us3val1.X);
                    var uva3l2 = pval1 / tval1 * (Convert.ToDouble(us3val2.Y) - Convert.ToDouble(us3val1.Y)) + Convert.ToDouble(us3val1.Y);
                    var uva3l3 = pval1 / tval1 * (Convert.ToDouble(us3val2.Z) - Convert.ToDouble(us3val1.Z)) + Convert.ToDouble(us3val1.Z);
                    return new UIntPoint3Data((uint)uv3al1, (uint)uva3l2, (uint)uva3l3);
                case "LongPoint3Data":
                    var lpsval1 = (LongPoint3Data)((object)value1);
                    var lpsval2 = (LongPoint3Data)((object)value2);
                    var lpval1 = pval1 / tval1 * (Convert.ToDouble(lpsval2.X) - Convert.ToDouble(lpsval1.X)) + Convert.ToDouble(lpsval1.X);
                    var lpval2 = pval1 / tval1 * (Convert.ToDouble(lpsval2.Y) - Convert.ToDouble(lpsval1.Y)) + Convert.ToDouble(lpsval1.Y);
                    var lpval3 = pval1 / tval1 * (Convert.ToDouble(lpsval2.Z) - Convert.ToDouble(lpsval1.Z)) + Convert.ToDouble(lpsval1.Z);
                    return new LongPoint3Data((long)lpval1, (long)lpval2, (long)lpval3);
                case "ULongPoint3Data":
                    var ulpsval1 = (ULongPoint3Data)((object)value1);
                    var ulpsval2 = (ULongPoint3Data)((object)value2);
                    var ulpval1 = pval1 / tval1 * (Convert.ToDouble(ulpsval2.X) - Convert.ToDouble(ulpsval1.X)) + Convert.ToDouble(ulpsval1.X);
                    var ulpval2 = pval1 / tval1 * (Convert.ToDouble(ulpsval2.Y) - Convert.ToDouble(ulpsval1.Y)) + Convert.ToDouble(ulpsval1.Y);
                    var ulpval3 = pval1 / tval1 * (Convert.ToDouble(ulpsval2.Z) - Convert.ToDouble(ulpsval1.Z)) + Convert.ToDouble(ulpsval1.Z);
                    return new ULongPoint3Data((ulong)ulpval1, (ulong)ulpval2, (ulong)ulpval3);
            }

            return default(T);
            //var pval1 = (time - startTime).TotalMilliseconds;
            //var tval1 = (endTime - startTime).TotalMilliseconds;

            //if (typeof(T) == typeof(IntPointData))
            //{
            //    var sval1 = (IntPointData)((object)value1);
            //    var sval2 = (IntPointData)((object)value2);
            //    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
            //    var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
            //    return new IntPointData((int)val1, (int)val2);
            //}
            //else if (typeof(T) == typeof(UIntPointData))
            //{
            //    var sval1 = (UIntPointData)((object)value1);
            //    var sval2 = (UIntPointData)((object)value2);
            //    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
            //    var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
            //    return new UIntPointData((uint)val1, (uint)val2);
            //}
            //else if (typeof(T) == typeof(LongPointData))
            //{
            //    var sval1 = (LongPointData)((object)value1);
            //    var sval2 = (LongPointData)((object)value2);
            //    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
            //    var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
            //    return new LongPointData((long)val1, (long)val2);
            //}
            //else if (typeof(T) == typeof(ULongPointData))
            //{
            //    var sval1 = (ULongPointData)((object)value1);
            //    var sval2 = (ULongPointData)((object)value2);
            //    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
            //    var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
            //    return new ULongPointData((ulong)val1, (ulong)val2);
            //}
            //else if (typeof(T) == typeof(IntPoint3Data))
            //{
            //    var sval1 = (IntPoint3Data)((object)value1);
            //    var sval2 = (IntPoint3Data)((object)value2);
            //    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
            //    var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
            //    var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
            //    return new IntPoint3Data((int)val1, (int)val2, (int)val3);
            //}
            //else if (typeof(T) == typeof(UIntPoint3Data))
            //{
            //    var sval1 = (UIntPoint3Data)((object)value1);
            //    var sval2 = (UIntPoint3Data)((object)value2);
            //    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
            //    var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
            //    var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
            //    return new UIntPoint3Data((uint)val1, (uint)val2, (uint)val3);
            //}
            //else if (typeof(T) == typeof(LongPoint3Data))
            //{
            //    var sval1 = (LongPoint3Data)((object)value1);
            //    var sval2 = (LongPoint3Data)((object)value2);
            //    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
            //    var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
            //    var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
            //    return new LongPoint3Data((long)val1, (long)val2, (long)val3);
            //}
            //else if (typeof(T) == typeof(ULongPoint3Data))
            //{
            //    var sval1 = (ULongPoint3Data)((object)value1);
            //    var sval2 = (ULongPoint3Data)((object)value2);
            //    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
            //    var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
            //    var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
            //    return new ULongPoint3Data((ulong)val1, (ulong)val2, (ulong)val3);
            //}

            //return default(T);
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
                                                var sval1 = Convert.ToDouble(vals[i]);
                                                var sval2 = Convert.ToDouble(vals[i + 1]);
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
