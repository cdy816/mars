//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2021/01/14 9:00:01.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using DotNetty.Common;
using Google.Protobuf.Reflection;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace DBRuntime.His
{
    /// <summary>
    /// 
    /// </summary>
    public class HisDataMemoryQueryService3 : IHisQueryFromMemory
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        public static HisDataMemoryQueryService3 Service = new HisDataMemoryQueryService3();

        private Dictionary<long, SortedDictionary<DateTime,ManualTimeSpanMemory3>> mManualHisMemorys = new Dictionary<long, SortedDictionary<DateTime, ManualTimeSpanMemory3>>();

        private SortedDictionary<DateTime, TimeSpanMemory3> mHisMemorys = new SortedDictionary<DateTime, TimeSpanMemory3>();


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public HisEnginer3 HisEnginer { get; set; }

        

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            mHisMemorys.Clear();
            mManualHisMemorys.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="endtime"></param>
        public void RegistorMemory(DateTime startTime,DateTime endtime, HisDataMemoryBlockCollection3 memory)
        {
            lock (mHisMemorys)
            {
                if (mHisMemorys.ContainsKey(startTime))
                {
                    var vitem = mHisMemorys[startTime];
                    vitem.End = endtime;
                    vitem.Memory = memory;
                }
                else
                {
                    var vitem = new TimeSpanMemory3() { Start = startTime, End = endtime, Memory = memory };
                    mHisMemorys.Add(startTime, vitem);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        public void ClearMemoryTime(DateTime startTime)
        {
            lock (mHisMemorys)
            {
                if (mHisMemorys.ContainsKey(startTime))
                {
                    mHisMemorys.Remove(startTime);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="memory"></param>
        public void RegistorManual(long id,DateTime startTime,DateTime endTime, ManualHisDataMemoryBlock memory)
        {
            lock (mManualHisMemorys)
            {
                if (mManualHisMemorys.ContainsKey(id))
                {
                    var vv = mManualHisMemorys[id];
                    if (!vv.ContainsKey(startTime))
                    {
                        vv.Add(startTime, new ManualTimeSpanMemory3() { Start = startTime, End = endTime, Memory = memory });
                    }
                    else
                    {
                        var vitem = vv[startTime];
                        vitem.End = endTime;
                        vitem.Memory = memory;
                    }
                }
                else
                {
                    SortedDictionary<DateTime, ManualTimeSpanMemory3> mms = new SortedDictionary<DateTime, ManualTimeSpanMemory3>();
                    mms.Add(startTime, new ManualTimeSpanMemory3() { Start = startTime, End = endTime, Memory = memory });
                    mManualHisMemorys.Add(id, mms);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        public void ClearManualMemoryTime(int id, DateTime startTime)
        {
            lock (mManualHisMemorys)
            {
                if (mManualHisMemorys.ContainsKey(id))
                {
                    if (mManualHisMemorys[id].ContainsKey(startTime))
                    {
                        mManualHisMemorys[id].Remove(startTime);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private bool IsManualRecord(long id)
        {
            if (this.HisEnginer.HisTagManager.HisTags.ContainsKey(id))
            {
                return HisEnginer.HisTagManager.HisTags[id].Type == RecordType.Driver;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        public bool CheckTime(long id,DateTime time)
        {
            if(IsManualRecord(id))
            {
                lock (mManualHisMemorys)
                {
                    if (mManualHisMemorys.ContainsKey(id))
                    {
                        foreach (var vv in mManualHisMemorys[id])
                        {
                            if (vv.Value.Contains(time)) return true;
                        }
                    }
                }
                return false;
            }
            else
            {
                lock (mHisMemorys)
                {
                    foreach (var vv in mHisMemorys)
                    {
                        if (vv.Value.Contains(time)) return true;
                    }
                }
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public DateTime GetStartMemoryTime(long id)
        {
            if (IsManualRecord(id))
            {
                lock (mManualHisMemorys)
                {
                    if (mManualHisMemorys.ContainsKey(id))
                    {
                        return mManualHisMemorys[id].First().Key;
                    }
                }
                return DateTime.MinValue;
            }
            else
            {
                lock (mHisMemorys)
                {
                    if (mHisMemorys.Count > 0)
                        return mHisMemorys.First().Key;
                    else
                    {
                        return DateTime.MinValue;
                    }
                }
            }
        }


        /// <summary>
        /// 查询所有值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue<T>(int id, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            if (!IsManualRecord(id))
            {

                KeyValuePair<DateTime, TimeSpanMemory3>[] vhh;
                lock(mHisMemorys)
                vhh = mHisMemorys.ToArray();

                foreach(var vv in vhh)
                {
                    var vss = vv.Value.Cross(new DateTimeSpan() { Start = startTime, End = endTime });
                    if(!vss.IsEmpty())
                    {

                        if (!vv.Value.Memory.TagAddress.ContainsKey(id)) break;

                        var vmm = vv.Value.Memory.TagAddress[id];

                        var stim = (int)((vss.Start - vv.Value.Memory.CurrentDatetime).TotalMilliseconds / HisEnginer2.MemoryTimeTick);
                        var etim = (int)((vss.End - vv.Value.Memory.CurrentDatetime).TotalMilliseconds / HisEnginer2.MemoryTimeTick);
                        var tims = ReadTimer(stim, etim, vv.Value.Memory, vmm);

                        var vals = ReadValueInner<T>(vv.Value.Memory, tims.Keys.ToList(), 0, vv.Value.Memory.ReadValueOffsetAddress(vmm),vv.Value.Memory.ReadDataBaseAddress(vmm));

                        foreach(var vvk in tims)
                        {
                            var time = vv.Value.Memory.CurrentDatetime.AddMilliseconds(vvk.Value * HisEnginer2.MemoryTimeTick);
                            result.Add(vals[vvk.Key], time, vv.Value.Memory.ReadByte(vv.Value.Memory.ReadDataBaseAddress(vmm),vvk.Key + vv.Value.Memory.ReadQualityOffsetAddress(vmm)));
                        }

                    }
                }
            }
            else
            {
                KeyValuePair<DateTime, ManualTimeSpanMemory3>[] vhh;

                lock (mManualHisMemorys)
                {
                    if (!mManualHisMemorys.ContainsKey(id)) return;
                    vhh = mManualHisMemorys[id].ToArray();
                }

                foreach (var vv in vhh)
                {
                    var vss = vv.Value.Cross(new DateTimeSpan() { Start = startTime, End = endTime });
                    if (!vss.IsEmpty())
                    {
                        var vmm = vv.Value.Memory;

                        var stim = (int)((vss.Start - vv.Value.Memory.Time).TotalMilliseconds / HisEnginer2.MemoryTimeTick);
                        var etim = (int)((vss.End - vv.Value.Memory.Time).TotalMilliseconds / HisEnginer2.MemoryTimeTick);
                        var tims = ReadTimer2(stim, etim, vmm);

                        var vals = ReadValueInner<T>(vmm, tims.Keys.ToList(), 0, vmm.ValueAddress);

                        foreach (var vvk in tims)
                        {
                            var time = vv.Value.Memory.Time.AddMilliseconds(vvk.Value * HisEnginer2.MemoryTimeTick);
                            result.Add(vals[vvk.Key], time, vmm.ReadByte(vvk.Key + vmm.QualityAddress));
                        }

                    }
                }
            }
        }

        private Dictionary<int, int> ReadTimer(int start, int end,HisDataMemoryBlockCollection3  block,int index)
        {
            Dictionary<int, int> re = new Dictionary<int, int>();
            bool isStart = false;
            var vcount = block.ReadValueOffsetAddress(index)/2;
            var basedata = block.ReadDataBaseAddress(index);
            for (int i = 0; i < vcount; i++)
            {
                var val = block.ReadShort(basedata,i * 2);

                if (i != 0 && val == 0) continue;

                if (!isStart)
                {
                    if (val >= start)
                    {
                        isStart = true;
                        re.Add(i, val);
                    }
                }
                else
                {

                    if (val > end) break;
                    else
                    {
                        re.Add(i, val);
                    }
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="block"></param>
        /// <returns></returns>
        private Dictionary<int, int> ReadTimer2(int start, int end, HisDataMemoryBlock block)
        {
            Dictionary<int, int> re = new Dictionary<int, int>();
            bool isStart = false;
            var vcount = block.ValueAddress/4;
            for (int i = 0; i < vcount; i++)
            {
                var val = block.ReadInt(i*4);
                if (i != 0 && val == 0) continue;
                if (!isStart)
                {
                    if (val >= start)
                    {
                        isStart = true;
                        re.Add(i, val);
                    }
                }
                else
                {

                    if (val > end) break;
                    else
                    {
                        re.Add(i, val);
                    }
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="block"></param>
        private Tuple<int,int> ReadTimeToFit(int time,HisDataMemoryBlock block)
        {
            var vcount = block.ValueAddress/2;
            short prev = block.ReadShort(0);
            if (prev == time) return new Tuple<int, int>(0, 0);
            else if (time < prev) return new Tuple<int, int>(-1, -1);

            for(int i=1;i<vcount;i++)
            {
                var after = block.ReadShort(i * 2);
                if(time == after)
                {
                    return new Tuple<int, int>(after, after);
                }
                else if(time<after)
                {
                    return new Tuple<int, int>(i - 1, i);
                }
                prev = after;
            }
            return new Tuple<int, int>(-1, -1);
        }


        private Tuple<int, int> ReadTimeToFit(int time, HisDataMemoryBlockCollection3 block,int id)
        {
            var vcount = block.ReadValueOffsetAddress(id) / 2;
            var basedata = block.ReadDataBaseAddress(id);
            short prev = block.ReadShort(basedata, 0);
            if (prev == time) return new Tuple<int, int>(0, 0);
            else if (time < prev) return new Tuple<int, int>(-1, -1);

            for (int i = 1; i < vcount; i++)
            {
                var after = block.ReadShort(basedata, i * 2);
                if (time == after)
                {
                    return new Tuple<int, int>(after, after);
                }
                else if (time < after)
                {
                    return new Tuple<int, int>(i - 1, i);
                }
                prev = after;
            }
            return new Tuple<int, int>(-1, -1);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="block"></param>
        /// <returns></returns>
        private Tuple<int, int> ReadTimeToFit2(int time, HisDataMemoryBlock block)
        {
            var vcount = block.ValueAddress/4;
            int prev = block.ReadInt(0);
            if (prev == time) return new Tuple<int, int>(0, 0);
            else if (time < prev) return new Tuple<int, int>(-1, -1);

            for (int i = 1; i < vcount; i++)
            {
                var after = block.ReadInt(i * 4);
                if (time == after)
                {
                    return new Tuple<int, int>(after, after);
                }
                else if (time < after)
                {
                    return new Tuple<int, int>(i - 1, i);
                }
                prev = after;
            }
            return new Tuple<int, int>(-1, -1);
        }

        private List<object> ReadValueInner<T>(HisDataMemoryBlock datafile, List<int> valIndex, long offset, long valueaddr)
        {
            List<object> re = new List<object>();
            if (typeof(T) == typeof(bool))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(Convert.ToBoolean(datafile.ReadByte(offset + valueaddr + vv)));
                }
            }
            else if (typeof(T) == typeof(byte))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadByte(offset + valueaddr + vv));
                }

            }
            else if (typeof(T) == typeof(short))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadShort(offset + valueaddr + vv * 2));
                }

            }
            else if (typeof(T) == typeof(ushort))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((ushort)datafile.ReadShort(offset + valueaddr + vv * 2));
                }

            }
            else if (typeof(T) == typeof(int))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadInt(offset + valueaddr + vv * 4));
                }

            }
            else if (typeof(T) == typeof(uint))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((uint)datafile.ReadInt(offset + valueaddr + vv * 4));
                }

            }
            else if (typeof(T) == typeof(long))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((long)datafile.ReadLong(offset + valueaddr + vv * 8));
                }

            }
            else if (typeof(T) == typeof(ulong))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((ulong)datafile.ReadLong(offset + valueaddr + vv * 8));
                }

            }
            else if (typeof(T) == typeof(double))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadDouble(offset + valueaddr + vv * 8));
                }

            }
            else if (typeof(T) == typeof(float))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadFloat(offset + valueaddr + vv * 4));
                }

            }
            else if (typeof(T) == typeof(string))
            {
                foreach (var vv in valIndex)
                {
                    var str = Encoding.Unicode.GetString(datafile.ReadBytes(offset + valueaddr + vv * Const.StringSize, Const.StringSize));
                    re.Add(str);
                }

            }
            else if (typeof(T) == typeof(DateTime))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadDateTime(offset + valueaddr + vv * 8));
                }

            }
            else if (typeof(T) == typeof(IntPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = datafile.ReadInt(offset + valueaddr + vv * 8);
                    var y = datafile.ReadInt(offset + valueaddr + vv * 8 + 4);
                    re.Add(new IntPointData() { X = x, Y = y });
                }

            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = (uint)datafile.ReadInt(offset + valueaddr + vv * 8);
                    var y = (uint)datafile.ReadInt(offset + valueaddr + vv * 8 + 4);
                    re.Add(new UIntPointData() { X = x, Y = y });
                }

            }
            else if (typeof(T) == typeof(LongPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = (long)datafile.ReadLong(offset + valueaddr + vv * 16);
                    var y = (long)datafile.ReadLong(offset + valueaddr + vv * 16 + 8);
                    re.Add(new LongPointData() { X = x, Y = y });
                }

            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = (ulong)datafile.ReadLong(offset + valueaddr + vv * 16);
                    var y = (ulong)datafile.ReadLong(offset + valueaddr + vv * 16 + 8);
                    re.Add(new ULongPointData() { X = x, Y = y });
                }

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

            }
            return re;
        }

        private List<object> ReadValueInner<T>(HisDataMemoryBlockCollection3 datafile, List<int> valIndex, long offset, long valueaddr,long address)
        {
            List<object> re = new List<object>();
            if (typeof(T) == typeof(bool))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(Convert.ToBoolean(datafile.ReadByte(address, offset + valueaddr + vv)));
                }
            }
            else if (typeof(T) == typeof(byte))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadByte(address, offset + valueaddr + vv));
                }
               
            }
            else if (typeof(T) == typeof(short))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadShort(address, offset + valueaddr + vv * 2));
                }
               
            }
            else if (typeof(T) == typeof(ushort))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((ushort)datafile.ReadShort(address, offset + valueaddr + vv * 2));
                }
                
            }
            else if (typeof(T) == typeof(int))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadInt(address, offset + valueaddr + vv * 4));
                }
               
            }
            else if (typeof(T) == typeof(uint))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((uint)datafile.ReadInt(address, offset + valueaddr + vv * 4));
                }
                
            }
            else if (typeof(T) == typeof(long))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((long)datafile.ReadLong(address,offset + valueaddr + vv * 8));
                }
              
            }
            else if (typeof(T) == typeof(ulong))
            {
                foreach (var vv in valIndex)
                {
                    re.Add((ulong)datafile.ReadLong(address, offset + valueaddr + vv * 8));
                }
                
            }
            else if (typeof(T) == typeof(double))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadDouble(address, offset + valueaddr + vv * 8));
                }
             
            }
            else if (typeof(T) == typeof(float))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadFloat(address, offset + valueaddr + vv * 4));
                }
               
            }
            else if (typeof(T) == typeof(string))
            {
                foreach (var vv in valIndex)
                {
                    var str = Encoding.Unicode.GetString(datafile.ReadBytes(address, offset + valueaddr + vv * Const.StringSize, Const.StringSize));
                    re.Add(str);
                }
               
            }
            else if (typeof(T) == typeof(DateTime))
            {
                foreach (var vv in valIndex)
                {
                    re.Add(datafile.ReadDateTime(address, offset + valueaddr + vv * 8));
                }
               
            }
            else if (typeof(T) == typeof(IntPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = datafile.ReadInt(address, offset + valueaddr + vv * 8);
                    var y = datafile.ReadInt(address, offset + valueaddr + vv * 8 + 4);
                    re.Add(new IntPointData() { X = x, Y = y });
                }
              
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = (uint)datafile.ReadInt(address, offset + valueaddr + vv * 8);
                    var y = (uint)datafile.ReadInt(address, offset + valueaddr + vv * 8 + 4);
                    re.Add(new UIntPointData() { X = x, Y = y });
                }
               
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = (long)datafile.ReadLong(address, offset + valueaddr + vv * 16);
                    var y = (long)datafile.ReadLong(address, offset + valueaddr + vv * 16 + 8);
                    re.Add(new LongPointData() { X = x, Y = y });
                }
              
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                foreach (var vv in valIndex)
                {
                    var x = (ulong)datafile.ReadLong(address, offset + valueaddr + vv * 16);
                    var y = (ulong)datafile.ReadLong(address, offset + valueaddr + vv * 16 + 8);
                    re.Add(new ULongPointData() { X = x, Y = y });
                }
                
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                foreach (var vv in valIndex)
                {
                    var x = datafile.ReadInt(address, offset + valueaddr + vv * 12);
                    var y = datafile.ReadInt(address, offset + valueaddr + vv * 12 + 4);
                    var z = datafile.ReadInt(address, offset + valueaddr + vv * 12 + 8);
                    re.Add(new IntPoint3Data() { X = x, Y = y, Z = z });
                }
              
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                foreach (var vv in valIndex)
                {
                    var x = (uint)datafile.ReadInt(address, offset + valueaddr + vv * 12);
                    var y = (uint)datafile.ReadInt(address, offset + valueaddr + vv * 12 + 4);
                    var z = (uint)datafile.ReadInt(address, offset + valueaddr + vv * 12 + 8);
                    re.Add(new UIntPoint3Data() { X = x, Y = y, Z = z });
                }
              
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                foreach (var vv in valIndex)
                {
                    var x = (long)datafile.ReadLong(address, offset + valueaddr + vv * 24);
                    var y = (long)datafile.ReadLong(address, offset + valueaddr + vv * 24 + 8);
                    var z = (long)datafile.ReadLong(address, offset + valueaddr + vv * 24 + 168);
                    re.Add(new LongPoint3Data() { X = x, Y = y, Z = z });
                }
               
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                foreach (var vv in valIndex)
                {
                    var x = (ulong)datafile.ReadLong(address, offset + valueaddr + vv * 24);
                    var y = (ulong)datafile.ReadLong(address, offset + valueaddr + vv * 24 + 8);
                    var z = (ulong)datafile.ReadLong(address, offset + valueaddr + vv * 24 + 168);
                    re.Add(new ULongPoint3Data() { X = x, Y = y, Z = z });
                }
                
            }
            return re;
        }

        /// <summary>
        /// 查询指定时刻的值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue<T>(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            if(!IsManualRecord(id))
            {
                KeyValuePair<DateTime, TimeSpanMemory3>[] vhh;
                lock (mHisMemorys)
                    vhh = mHisMemorys.ToArray();

                foreach (var vtime in times)
                {
                    foreach (var vv in vhh)
                    {
                        if (vv.Value.Contains(vtime))
                        {
                            if (vv.Value.Memory.TagAddress.ContainsKey(id))
                            {
                                var tim = (int)((vtime - vv.Value.Memory.CurrentDatetime).TotalMilliseconds / HisEnginer2.MemoryTimeTick);
                                var vmm = vv.Value.Memory.TagAddress[id];
                                var timeindx = ReadTimeToFit(tim, vv.Value.Memory, vmm);
                                if (timeindx.Item1 > -1 && timeindx.Item2 > -1)
                                {
                                    if (timeindx.Item1 == timeindx.Item2)
                                    {
                                        var vals = ReadValueInner<T>(vv.Value.Memory, new List<int>() { timeindx.Item1 }, 0, vv.Value.Memory.ReadValueOffsetAddress(vmm), vv.Value.Memory.ReadDataBaseAddress(vmm));
                                        var qua = vv.Value.Memory.ReadByte(vv.Value.Memory.ReadDataBaseAddress(vmm), vv.Value.Memory.ReadQualityOffsetAddress(vmm) + timeindx.Item1);
                                        result.Add(vals[0], vtime, qua);
                                    }
                                    else
                                    {
                                        var vals = ReadValueInner<T>(vv.Value.Memory, new List<int>() { timeindx.Item1, timeindx.Item2 }, 0, vv.Value.Memory.ReadValueOffsetAddress(vmm), vv.Value.Memory.ReadDataBaseAddress(vmm));
                                        var qua1 = vv.Value.Memory.ReadByte(vv.Value.Memory.ReadDataBaseAddress(vmm), vv.Value.Memory.ReadQualityOffsetAddress(vmm) + timeindx.Item1);
                                        var qua2 = vv.Value.Memory.ReadByte(vv.Value.Memory.ReadDataBaseAddress(vmm), vv.Value.Memory.ReadQualityOffsetAddress(vmm) + timeindx.Item2);
                                        var time1 = vv.Value.Memory.CurrentDatetime.AddMilliseconds(vv.Value.Memory.ReadShort(vv.Value.Memory.ReadDataBaseAddress(vmm), timeindx.Item1) * HisEnginer2.MemoryTimeTick);
                                        var time2 = vv.Value.Memory.CurrentDatetime.AddMilliseconds(vv.Value.Memory.ReadShort(vv.Value.Memory.ReadDataBaseAddress(vmm), timeindx.Item2) * HisEnginer2.MemoryTimeTick);
                                        switch (type)
                                        {
                                            case QueryValueMatchType.Previous:
                                                result.Add(vals[0], vtime, qua1);
                                                break;
                                            case QueryValueMatchType.After:
                                                result.Add(vals[1], vtime, qua2);
                                                break;
                                            case QueryValueMatchType.Linear:
                                                if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                                                {
                                                    var ppval = (vtime - time1).TotalMilliseconds;
                                                    var ffval = (time2 - vtime).TotalMilliseconds;

                                                    if (ppval < ffval)
                                                    {
                                                        result.Add(vals[0], vtime, qua1);
                                                    }
                                                    else
                                                    {
                                                        result.Add(vals[1], vtime, qua2);
                                                    }
                                                }
                                                else
                                                {

                                                    if (qua1 < 20 && qua2 < 20)
                                                    {
                                                        if (CheckTypeIsPointData(typeof(T)))
                                                        {
                                                            result.Add(LinerValue(time1, time2, vtime, vals[0], vals[1]), vtime, 0);
                                                        }
                                                        else
                                                        {
                                                            var pval1 = (vtime - time1).TotalMilliseconds;
                                                            var tval1 = (time2 - time1).TotalMilliseconds;
                                                            var sval1 = (double)vals[0];
                                                            var sval2 = (double)vals[1];
                                                            var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                                            result.Add((object)val1, vtime, 0);
                                                        }
                                                    }
                                                    else if (qua1 < 20)
                                                    {
                                                        result.Add(vals[0], vtime, qua1);
                                                    }
                                                    else if (qua2 < 20)
                                                    {
                                                        result.Add(vals[1], vtime, qua2);
                                                    }
                                                    else
                                                    {
                                                        result.Add(default(T), time1, (byte)QualityConst.Null);
                                                    }
                                                }
                                                break;
                                            case QueryValueMatchType.Closed:
                                                var pval = (vtime - time1).TotalMilliseconds;
                                                var fval = (time2 - vtime).TotalMilliseconds;

                                                if (pval < fval)
                                                {
                                                    result.Add(vals[0], time1, qua1);
                                                }
                                                else
                                                {
                                                    result.Add(vals[1], time1, qua2);
                                                }
                                                break;
                                        }
                                    }
                                }
                            }
                            break;
                        }
                    }
                }
            }
            else
            {
                KeyValuePair<DateTime, ManualTimeSpanMemory3>[] vhh;

                lock (mManualHisMemorys)
                {
                    if (!mManualHisMemorys.ContainsKey(id)) return;
                    vhh = mManualHisMemorys[id].ToArray();
                }

                foreach (var vtime in times)
                {
                    foreach (var vv in vhh)
                    {
                        if (vv.Value.Contains(vtime))
                        {
                            var tim = (int)((vtime - vv.Value.Memory.Time).TotalMilliseconds / HisEnginer2.MemoryTimeTick);
                            var vmm = vv.Value.Memory;
                            var timeindx = ReadTimeToFit2(tim, vmm);
                            if (timeindx.Item1 > -1 && timeindx.Item2 > -1)
                            {
                                if (timeindx.Item1 == timeindx.Item2)
                                {
                                    var vals = ReadValueInner<T>(vmm, new List<int>() { timeindx.Item1 }, 0, vmm.ValueAddress);
                                    var qua = vmm.ReadByte(vmm.QualityAddress + timeindx.Item1);
                                    result.Add(vals[0], vtime, qua);
                                }
                                else
                                {
                                    var vals = ReadValueInner<T>(vmm, new List<int>() { timeindx.Item1, timeindx.Item2 }, 0, vmm.ValueAddress);
                                    var qua1 = vmm.ReadByte(vmm.QualityAddress + timeindx.Item1);
                                    var qua2 = vmm.ReadByte(vmm.QualityAddress + timeindx.Item2);
                                    var time1 = vv.Value.Memory.Time.AddMilliseconds(vmm.ReadInt(timeindx.Item1) * HisEnginer2.MemoryTimeTick);
                                    var time2 = vv.Value.Memory.Time.AddMilliseconds(vmm.ReadInt(timeindx.Item2) * HisEnginer2.MemoryTimeTick);
                                    switch (type)
                                    {
                                        case QueryValueMatchType.Previous:
                                            result.Add(vals[0], vtime, qua1);
                                            break;
                                        case QueryValueMatchType.After:
                                            result.Add(vals[1], vtime, qua2);
                                            break;
                                        case QueryValueMatchType.Linear:
                                            if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                                            {
                                                var ppval = (vtime - time1).TotalMilliseconds;
                                                var ffval = (time2 - vtime).TotalMilliseconds;

                                                if (ppval < ffval)
                                                {
                                                    result.Add(vals[0], vtime, qua1);
                                                }
                                                else
                                                {
                                                    result.Add(vals[1], vtime, qua2);
                                                }
                                            }
                                            else
                                            {

                                                if (qua1 < 20 && qua2 < 20)
                                                {
                                                    if (CheckTypeIsPointData(typeof(T)))
                                                    {
                                                        result.Add(LinerValue(time1, time2, vtime, vals[0], vals[1]), vtime, 0);
                                                    }
                                                    else
                                                    {
                                                        var pval1 = (vtime - time1).TotalMilliseconds;
                                                        var tval1 = (time2 - time1).TotalMilliseconds;
                                                        var sval1 = (double)vals[0];
                                                        var sval2 = (double)vals[1];
                                                        var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                                        result.Add((object)val1, vtime, 0);
                                                    }
                                                }
                                                else if (qua1 < 20)
                                                {
                                                    result.Add(vals[0], vtime, qua1);
                                                }
                                                else if (qua2 < 20)
                                                {
                                                    result.Add(vals[1], vtime, qua2);
                                                }
                                                else
                                                {
                                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                                }
                                            }
                                            break;
                                        case QueryValueMatchType.Closed:
                                            var pval = (vtime - time1).TotalMilliseconds;
                                            var fval = (time2 - vtime).TotalMilliseconds;

                                            if (pval < fval)
                                            {
                                                result.Add(vals[0], time1, qua1);
                                            }
                                            else
                                            {
                                                result.Add(vals[1], time1, qua2);
                                            }
                                            break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
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
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="time"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <returns></returns>
        private  object LinerValue<T>(DateTime startTime, DateTime endTime, DateTime time, T value1, T value2)
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

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }

    public class ManualTimeSpanMemory3 : DateTimeSpan
    {

        /// <summary>
        /// 
        /// </summary>
        public ManualHisDataMemoryBlock Memory { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class TimeSpanMemory3 : DateTimeSpan
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
        public HisDataMemoryBlockCollection3 Memory { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

}
