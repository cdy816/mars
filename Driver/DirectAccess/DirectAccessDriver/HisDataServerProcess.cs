//==============================================================
//  Copyright (C) 2022  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/1/2 11:00:57.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using Cdy.Tag.Driver;
using Cheetah;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DirectAccessDriver
{
    /// <summary>
    /// 
    /// </summary>
    public class HisDataServerProcess : ServerProcessBase
    {

        #region ... Variables  ...
        private bool mIsBusy = false;

       private  Dictionary<string,int> mExecutingTasks = new Dictionary<string, int>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public override byte FunId => APIConst.HisValueFun;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        public override void CheckDataBusy(string client)
        {
            if (mDatasCach.ContainsKey(client) && mDatasCach[client].Count > 100)
            {
                Parent.AsyncCallback(client, ToByteBuffer(APIConst.AysncReturn, APIConst.HisServerBusy));
                mIsBusy = true;
            }
            else if (mIsBusy)
            {
                mIsBusy = false;
                Parent.AsyncCallback(client, ToByteBuffer(APIConst.AysncReturn, APIConst.HisServerNoBusy));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        public override void ProcessSingleData(string client, ByteBuffer data)
        {
            if (data.RefCount == 0)
            {
                LoggerService.Service.Warn("DirectAccessDriver_HisDataServerProcess", "invailed data buffer in HisDataServerProcess");
                return;
            }

            //如果暂停，则不处理数据
            if (IsPause)
            {
                //to do 需要通知采集端，系统已经暂停
                //Parent.AsyncCallback(client, FunId, new byte[1], 0);
                return;
            }

            byte cmd = data.ReadByte();
            if (cmd < 160)
            {
                long id = data.ReadLong();
                if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
                {
                    try
                    {
                        switch (cmd)
                        {
                            case APIConst.SetTagHisValue:                                   
                                    try
                                    {
                                        ProcessSetHisData(client, data);
                                    }
                                    catch
                                    {

                                    }

                                    data.UnlockAndReturn();
                                return;
                            case APIConst.SetTagHisValue2:                                    
                                    try
                                    {
                                        ProcessSetHisData2(client, data);
                                    }
                                    catch
                                    {

                                    }

                                    data.UnlockAndReturn();
                                return;
                            case APIConst.SetAreaTagHisValue:
                                try
                                {
                                    ProcessSetAreaHisData(client, data);
                                }
                                catch
                                { }
                                data.UnlockAndReturn();
                                break;
                            case APIConst.SetAreaTagHisValue2:
                                try
                                {
                                    ProcessSetAreaHisData2(client, data);
                                }
                                catch
                                { }
                                data.UnlockAndReturn();
                                break;
                            case APIConst.ReadTagAllHisValue:
                                Task.Run(() =>
                                {
                                    ProcessRequestAllHisDataByMemory(client, data);
                                    data.UnlockAndReturn();
                                });
                                return;
                            case APIConst.ReadHisValueBySQL:
                                Task.Run(() =>
                                {
                                    ProcessQueryBySQL(client, data);
                                    data.UnlockAndReturn();
                                });
                                return;
                            case APIConst.ReadHisDataByTimeSpan:
                                Task.Run(() => { ProcessRequestHisDataByTimeSpanByMemory(client, data); data.UnlockAndReturn(); });
                                return;
                            case APIConst.ReadHisDataByTimeSpanByIgnorClosedQuality:
                                Task.Run(() => { ProcessRequestHisDataByTimeSpanByMemoryByIgnorClosedQuality(client, data); data.UnlockAndReturn(); });
                                return;
                            case APIConst.ReadHisDatasByTimePoint:
                                Task.Run(() => { ProcessRequestHisDatasByTimePoint(client, data); data.UnlockAndReturn(); });
                                return;
                            case APIConst.ReadHisDatasByTimePointByIgnorClosedQuality:
                                Task.Run(() => { ProcessRequestHisDatasByTimePointByIgnoreClosedQualtiy(client, data); data.UnlockAndReturn(); });
                                return;
                        }
                    }
                    catch (Exception ex)
                    {
                        LoggerService.Service.Warn("DirectAccessDriver HisDataServerProcess", ex.Message);
                    }
                }
            }
            else
            {
                string user = data.ReadString();
                string pass = data.ReadString();
                if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().LoginOnce(user, pass))
                {
                    try
                    {
                        switch (cmd)
                        {
                            case APIConst.SetTagHisValueWithUser:
                                lock (mExecutingTasks)
                                {
                                    if (mExecutingTasks.ContainsKey(client) && mExecutingTasks[client] > 1)
                                    {
                                        LoggerService.Service.Warn("DirectAccess", $"HisDataServerProcess {client} 设置历史数据忙，本次设置无效");
                                        return;
                                    }
                                }

                                Task.Run(() =>
                                {
                                    lock (mExecutingTasks)
                                    {
                                        if (mExecutingTasks.ContainsKey(client))
                                        {
                                            mExecutingTasks[client]++;
                                        }
                                        else
                                        {
                                            mExecutingTasks.Add(client, 1);
                                        }
                                    }
                                    try
                                    {
                                        ProcessSetHisDataWithUser(client, data);
                                    }
                                    catch
                                    {

                                    }
                                    data.UnlockAndReturn();
                                    lock (mExecutingTasks)
                                    {
                                        if (mExecutingTasks.ContainsKey(client))
                                        {
                                            mExecutingTasks[client]--;
                                        }
                                    }

                                });

                                return;
                            case APIConst.SetTagHisValueWithUser2:
                                lock (mExecutingTasks)
                                {
                                    if (mExecutingTasks.ContainsKey(client) && mExecutingTasks[client] > 1)
                                    {
                                        LoggerService.Service.Warn("DirectAccess", $"HisDataServerProcess {client} 设置历史数据忙，本次设置无效");
                                        return;
                                    }
                                }

                                Task.Run(() =>
                                {
                                    lock (mExecutingTasks)
                                    {
                                        if (mExecutingTasks.ContainsKey(client))
                                        {
                                            mExecutingTasks[client]++;
                                        }
                                        else
                                        {
                                            mExecutingTasks.Add(client, 1);
                                        }
                                    }

                                    try
                                    {
                                        ProcessSetHisData2WithUser(client, data);
                                    }
                                    catch
                                    {

                                    }
                                    data.UnlockAndReturn();

                                    lock (mExecutingTasks)
                                    {
                                        if (mExecutingTasks.ContainsKey(client))
                                        {
                                            mExecutingTasks[client]--;
                                        }
                                    }

                                });

                                return;
                          
                        }
                    }
                    catch (Exception ex)
                    {
                        LoggerService.Service.Warn("DirectAccessDriver HisDataServerProcess", ex.Message);
                    }
                }
            }
            base.ProcessSingleData(client, data);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="mLastTime"></param>
        /// <param name="mSpanCount"></param>
        private void CheckAndDelay(DateTime dt,ref DateTime mLastTime, ref int mSpanCount)
        {
            if ((dt - mLastTime).TotalMinutes >= 5)
            {
                if (mLastTime != DateTime.MinValue)
                {
                    ServiceLocator.Locator.Resolve<ITagHisValueProduct>()?.SubmitCach();
                    mSpanCount++;
                    if (mSpanCount > 12 * 24 * 7)
                    {
                        Thread.Sleep(2000);
                        mSpanCount = 0;
                    }
                }
                mLastTime = dt;
            }
        }

        private void ProcessSetAreaHisDataInner(string clientid, ByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();
            var realservice = ServiceLocator.Locator.Resolve<IRealTagProduct>();

            DateTime time = block.ReadDateTime();
            var count = block.ReadInt();
            Dictionary<int,Tuple< object,byte>> vals = new Dictionary<int, Tuple<object, byte>>();
            for(int i=0;i<count;i++)
            {
                var id = block.ReadInt();
                var typ = block.ReadByte();
                bool ignor = !Driver.DriverdRecordTags.Contains(id) || !Driver.AllowTagIds.Contains(id);
                switch (typ)
                {
                    case (byte)TagType.Bool:

                        var bval = block.ReadByte();
                        var qa = block.ReadByte();
                        if (ignor) continue;
                        if(!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(bval, qa));
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        
                        break;
                    case (byte)TagType.Byte:
                         bval = block.ReadByte();
                         qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(bval, qa));
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        break;
                    case (byte)TagType.Short:
                        var sval = block.ReadShort();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(sval, qa));
                            realservice.SetTagValue(id, ref sval, qa);
                        }
                        break;
                    case (byte)TagType.UShort:
                        var usval = block.ReadUShort();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(usval, qa));
                            realservice.SetTagValue(id, ref usval, qa);
                        }
                        break;
                    case (byte)TagType.Int:
                        var ival = block.ReadInt();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(ival, qa));
                            realservice.SetTagValue(id, ref ival, qa);
                        }
                        break;
                    case (byte)TagType.UInt:
                        var uival = block.ReadUInt();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(uival, qa));
                            realservice.SetTagValue(id, ref uival, qa);
                        }
                        break;
                    case (byte)TagType.Long:
                        var lval = block.ReadLong();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(lval, qa));
                            realservice.SetTagValue(id, ref lval, qa);
                        }
                        break;
                    case (byte)TagType.ULong:
                        var ulval = block.ReadLong();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(ulval, qa));
                            realservice.SetTagValue(id, ref ulval, qa);
                        }
                        break;
                    case (byte)TagType.Float:
                        var fval = block.ReadFloat();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(fval, qa));
                            realservice.SetTagValue(id, ref fval, qa);
                        }
                        break;
                    case (byte)TagType.Double:
                        var dval = block.ReadDouble();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(dval, qa));
                            realservice.SetTagValue(id, ref dval, qa);
                        }
                        break;
                    case (byte)TagType.String:
                        var stval = block.ReadString();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(stval, qa));
                            realservice.SetTagValue(id, stval, qa);
                        }
                        break;
                    case (byte)TagType.DateTime:
                        var dtval = block.ReadDouble();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(dtval, qa));
                            realservice.SetTagValue(id, ref dtval, qa);
                        }
                        break;
                    case (byte)TagType.IntPoint:
                        var bval1 = block.ReadInt();
                        var bval2 = block.ReadInt();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(new object[] { bval1, bval2 }, qa));
                            var ipd = new IntPointData(bval1, bval2);
                            realservice.SetTagValue(id, ref ipd, qa);
                        }
                        break;
                    case (byte)TagType.UIntPoint:

                        var ubval1 = block.ReadUInt();
                        var ubval2 = block.ReadUInt();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(new object[] { ubval1, ubval1 }, qa));
                            var uipd = new UIntPointData(ubval1, ubval2);
                            realservice.SetTagValue(id, ref uipd, qa);
                        }
                        break;
                    case (byte)TagType.IntPoint3:
                        bval1 = block.ReadInt();
                        bval2 = block.ReadInt();
                        var bval3 = block.ReadInt();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(new object[] { bval1, bval2, bval3 }, qa));
                            var i3pd = new IntPoint3Data(bval1, bval2, bval3);
                            realservice.SetTagValue(id, ref i3pd, qa);
                        }

                        break;
                    case (byte)TagType.UIntPoint3:

                        ubval1 = block.ReadUInt();
                        ubval2 = block.ReadUInt();
                        var ubval3 = block.ReadUInt();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(new object[] { ubval1, ubval2, ubval3 }, qa));
                            var ui3pd = new UIntPoint3Data(ubval1, ubval2, ubval3);
                            realservice.SetTagValue(id, ref ui3pd, qa);
                        }

                        break;
                    case (byte)TagType.LongPoint:

                        var lval1 = block.ReadLong();
                        var lval2 = block.ReadLong();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(new object[] { lval1, lval2 }, qa));
                            var lpd = new LongPointData(lval1, lval2);
                            realservice.SetTagValue(id, ref lpd, qa);
                        }

                        break;
                    case (byte)TagType.ULongPoint:

                        var ulval1 = block.ReadULong();
                        var ulval2 = block.ReadULong();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(new object[] { ulval1, ulval2 }, qa));
                            var ulpd = new ULongPointData(ulval1, ulval2);
                            realservice.SetTagValue(id, ref ulpd, qa);
                        }
                        break;
                    case (byte)TagType.LongPoint3:
                        lval1 = block.ReadLong();
                        lval2 = block.ReadLong();
                        var lval3  = block.ReadLong();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(new object[] { lval1, lval2, lval3 }, qa));
                            var lpd3 = new LongPoint3Data(lval1, lval2, lval3);
                            realservice.SetTagValue(id, ref lpd3, qa);
                        }
                        break;
                    case (byte)TagType.ULongPoint3:

                        ulval1 = block.ReadULong();
                        ulval2 = block.ReadULong();
                        var ulval3 = block.ReadULong();
                        qa = block.ReadByte();
                        if (ignor) continue;
                        if (!vals.ContainsKey(id))
                        {
                            vals.Add(id, new Tuple<object, byte>(new object[] { ulval1, ulval2, ulval3 }, qa));
                            var ulpd3 = new ULongPoint3Data(ulval1, ulval2, ulval3);
                            realservice.SetTagValue(id, ref ulpd3, qa);
                        }
                        break;
                }

            }
            service.SetAreaTagHisValue(time, vals);

            service?.SubmitCach();
        }

        private void ProcessSetHisDataInner(string clientid,ByteBuffer block,out int spanCount)
        {
            var id = block.ReadInt();
            var count = block.ReadInt();
            var typ = block.ReadByte();
            //int timedu = block.ReadInt();
            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();
            var realservice = ServiceLocator.Locator.Resolve<IRealTagProduct>();

            bool ignor = !Driver.DriverdRecordTags.Contains(id) || !Driver.AllowTagIds.Contains(id);

            DateTime mLastTime = DateTime.MinValue;
            int mSpanCount = 0;
            switch (typ)
            {
                case (byte)TagType.Bool:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();

                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);

                        CheckAndDelay(dt,ref mLastTime,ref mSpanCount);

                    }
                    break;
                case (byte)TagType.Byte:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();
                        
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;

                        service.SetTagHisValue(id, dt, bval, qa);

                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);

                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Short:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadShort();
                        var qa = block.ReadByte();
                        
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        //  service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.UShort:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadUShort();
                        var qa = block.ReadByte();
                        
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Int:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadInt();
                        var qa = block.ReadByte();
                       
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.UInt:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadUInt();
                        var qa = block.ReadByte();
                        
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Long:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadLong();
                        var qa = block.ReadByte();

                        
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.ULong:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = (ulong)block.ReadLong();
                        var qa = block.ReadByte();
                        
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Float:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadFloat();
                        var qa = block.ReadByte();
                        
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Double:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadDouble();
                        var qa = block.ReadByte();
                        
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.String:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadString();
                        var qa = block.ReadByte();
                        
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.DateTime:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = DateTime.FromBinary(block.ReadLong());
                        var qa = block.ReadByte();
                        
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(id, ref bval, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, bval, qa);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.IntPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval1 = block.ReadInt();
                        var bval2 = block.ReadInt();
                        var qa = block.ReadByte();
                        
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            var ipd = new IntPointData(bval1, bval2);
                            realservice.SetTagValue(id, ref ipd, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, qa, bval1, bval2);

                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.UIntPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        //var bval = new UIntPointData() { X = block.ReadUnsignedInt(), Y = block.ReadUnsignedInt() };
                        var bval1 = block.ReadUInt();
                        var bval2 = block.ReadUInt();
                        var qa = block.ReadByte();
                        
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            var ipd = new UIntPointData(bval1, bval2);
                            realservice.SetTagValue(id, ref ipd, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, qa, bval1, bval2);

                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.IntPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        // var bval = new IntPoint3Data() { X = block.ReadInt(), Y = block.ReadInt(),Z=block.ReadInt() };
                        var bval1 = block.ReadInt();
                        var bval2 = block.ReadInt();
                        var bval3 = block.ReadInt();
                        var qa = block.ReadByte();
                        

                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            var ipd = new IntPoint3Data(bval1, bval2, bval3);
                            realservice.SetTagValue(id, ref ipd, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, qa, bval1, bval2, bval3);

                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.UIntPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        // var bval = new UIntPoint3Data() { X = block.ReadUnsignedInt(), Y = block.ReadUnsignedInt(), Z = block.ReadUnsignedInt() };
                        var bval1 = block.ReadUInt();
                        var bval2 = block.ReadUInt();
                        var bval3 = block.ReadUInt();
                        var qa = block.ReadByte();
                        

                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            var ipd = new UIntPoint3Data(bval1, bval2, bval3);
                            realservice.SetTagValue(id, ref ipd, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, qa, bval1, bval2, bval3);

                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.LongPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        // var bval = new LongPointData() { X = block.ReadLong(), Y = block.ReadLong() };
                        var bval1 = block.ReadLong();
                        var bval2 = block.ReadLong();
                        var qa = block.ReadByte();
                        
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            var ipd = new LongPointData(bval1, bval2);
                            realservice.SetTagValue(id, ref ipd, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, qa, bval1, bval2);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.ULongPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        //  var bval = new ULongPointData() { X = (ulong)block.ReadLong(), Y = (ulong)block.ReadLong() };
                        var bval1 = (ulong)block.ReadLong();
                        var bval2 = (ulong)block.ReadLong();
                        var qa = block.ReadByte();
                       
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            var ipd = new ULongPointData(bval1, bval2);
                            realservice.SetTagValue(id, ref ipd, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, qa, bval1, bval2);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.LongPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        // var bval = new LongPoint3Data() { X = block.ReadLong(), Y = block.ReadLong(),Z=block.ReadLong() };
                        var bval1 = block.ReadLong();
                        var bval2 = block.ReadLong();
                        var bval3 = block.ReadLong();
                        var qa = block.ReadByte();
                       
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            var ipd = new LongPoint3Data(bval1, bval2, bval3);
                            realservice.SetTagValue(id, ref ipd, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, qa, bval1, bval2, bval3);

                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.ULongPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        //  var bval = new ULongPoint3Data() { X = (ulong)block.ReadLong(), Y = (ulong)block.ReadLong(), Z = (ulong)block.ReadLong() };
                        var bval1 = (ulong)block.ReadLong();
                        var bval2 = (ulong)block.ReadLong();
                        var bval3 = (ulong)block.ReadLong();
                        var qa = block.ReadByte();
                        
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            var ipd = new ULongPoint3Data(bval1, bval2, bval3);
                            realservice.SetTagValue(id, ref ipd, qa);
                        }
                        if (ignor) continue;
                        service.SetTagHisValue(id, dt, qa, bval1, bval2, bval3);
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
            }
            spanCount = mSpanCount;

            service?.SubmitCach();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisData(string clientid, ByteBuffer block)
        {
            //if (CheckBlock(clientid)) return;

            ProcessSetHisDataInner(clientid, block,out int spanCount);
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetAreaHisData(string clientid, ByteBuffer block)
        {
            ProcessSetAreaHisDataInner(clientid, block);
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        private SqlExpress ParseSql(string sql,out List<int> selecttag, out Dictionary<int, byte> tagtps)
        {
            var sqlexp = new SqlExpress().FromString(sql);
            List<string> ls = new List<string>();
            var serice = ServiceLocator.Locator.Resolve<ITagManager>();

            if (sqlexp.Select.Selects.Count>0 && sqlexp.Select.Selects[0].TagName=="*" && !string.IsNullOrEmpty(sqlexp.From))
            {
                var tags = serice.GetTagByArea(sqlexp.From);
                if(tags!=null && tags.Count>0)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach(var vv in  tags.Select(e=>e.FullName))
                    {
                        sb.Append(vv.ToString()+",");
                    }
                    sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
                    sql = sql.Replace("*", sb.ToString());

                    sqlexp = new SqlExpress().FromString(sql);
                }
            }
            Dictionary<int,byte> tps= new Dictionary<int, byte>();
            List<int> selids = new List<int>();
            if(sqlexp.Select!=null)
            {
                foreach(var vv in sqlexp.Select.Selects)
                {
                    var tag = serice.GetTagByName(vv.TagName);
                    if(!tps.ContainsKey(tag.Id))
                    {
                        tps.Add(tag.Id, (byte)tag.Type);
                    }
                    selids.Add(tag.Id);
                }
            }

            if (sqlexp.Where != null)
            {
                FillTagIds(sqlexp.Where, tps);
            }
            selecttag = selids;
            tagtps=tps;
            return sqlexp;
        }

        private void FillTagIds(ExpressFilter filter, Dictionary<int, byte> tps)
        {
            var serice = ServiceLocator.Locator.Resolve<ITagManager>();
            foreach (var vv in filter.Filters)
            {
                if(vv is ExpressFilter)
                {
                    FillTagIds(vv as ExpressFilter,tps);
                }
                else
                {
                    var fa = (vv as FilterAction);
                    if(fa.TagName.ToLower()=="time")
                    {
                        continue;
                    }
                    var tag = serice.GetTagByName(fa.TagName);
                    if(tag!=null)
                    {
                        fa.TagId = tag.Id;
                        if(!tps.ContainsKey(tag.Id))
                        {
                            tps.Add(tag.Id,(byte)tag.Type);
                        }
                    }
                    else
                    {
                        throw new Exception($"tag {fa.TagName} 不存在!");
                    }
                }
            }
        }

        /// <summary>
        /// 查询实时值
        /// </summary>
        /// <param name="selids"></param>
        /// <param name="id"></param>
        private ByteBuffer ProcessQueryRealValueBySql(List<int> selids,int id)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagProduct>();
            var re = Parent.Allocate(APIConst.ReadTagHisValueReturn, 10 + 8 * selids.Count);
            re.Write(id);
            re.Write((byte)APIConst.ReadHisValueBySQL);
            re.Write((byte)2);
            ProcessRealData(selids, selids.Count, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cc"></param>
        /// <param name="re"></param>
        private void ProcessRealData(List<int> cc, int len, ByteBuffer re)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagConsumer>();
            re.Write(len);
            DateTime tmp = DateTime.UtcNow;
            //foreach (var vv in cc)
            for (int i = 0; i < len; i++)
            {
                var vv = cc[i];

                re.Write(vv);

                byte qu, type;
                DateTime time;
                object value;

                if (service.IsComplexTag(vv))
                {
                    List<RealTagValueWithTimer> vals = new List<RealTagValueWithTimer>();
                    re.WriteByte((byte)TagType.Complex);

                    service.GetComplexTagValue(vv, vals);

                    re.Write(vals.Count);
                    foreach (var vtmp in vals)
                    {
                        re.Write(vtmp.Id);
                        re.Write((byte)vtmp.ValueType);
                        switch ((byte)vtmp.ValueType)
                        {
                            case (byte)TagType.Bool:
                                re.Write(Convert.ToByte(vtmp.Value));
                                break;
                            case (byte)TagType.Byte:
                                re.Write(Convert.ToByte(vtmp.Value));
                                break;
                            case (byte)TagType.Short:
                                re.Write(Convert.ToInt16(vtmp.Value));
                                break;
                            case (byte)TagType.UShort:
                                re.Write(Convert.ToUInt16(vtmp.Value));
                                break;
                            case (byte)TagType.Int:
                                re.Write(Convert.ToInt32(vtmp.Value));
                                break;
                            case (byte)TagType.UInt:
                                re.Write(Convert.ToUInt32(vtmp.Value));
                                break;
                            case (byte)TagType.Long:
                            case (byte)TagType.ULong:
                                re.Write(Convert.ToInt64(vtmp.Value));
                                break;
                            case (byte)TagType.Float:
                                re.Write(Convert.ToSingle(vtmp.Value));
                                break;
                            case (byte)TagType.Double:
                                re.Write(Convert.ToDouble(vtmp.Value));
                                break;
                            case (byte)TagType.String:
                                string sval = vtmp.Value.ToString();
                                re.Write(sval);
                                //re.Write(sval.Length);
                                //re.Write(sval, Encoding.Unicode);
                                break;
                            case (byte)TagType.DateTime:
                                re.Write(((DateTime)vtmp.Value).Ticks);
                                break;
                            case (byte)TagType.IntPoint:
                                re.Write(((IntPointData)vtmp.Value).X);
                                re.Write(((IntPointData)vtmp.Value).Y);
                                break;
                            case (byte)TagType.UIntPoint:
                                re.Write((int)((UIntPointData)vtmp.Value).X);
                                re.Write((int)((UIntPointData)vtmp.Value).Y);
                                break;
                            case (byte)TagType.IntPoint3:
                                re.Write(((IntPoint3Data)vtmp.Value).X);
                                re.Write(((IntPoint3Data)vtmp.Value).Y);
                                re.Write(((IntPoint3Data)vtmp.Value).Z);
                                break;
                            case (byte)TagType.UIntPoint3:
                                re.Write((int)((UIntPoint3Data)vtmp.Value).X);
                                re.Write((int)((UIntPoint3Data)vtmp.Value).Y);
                                re.Write((int)((UIntPoint3Data)vtmp.Value).Z);
                                break;
                            case (byte)TagType.LongPoint:
                                re.Write(((LongPointData)vtmp.Value).X);
                                re.Write(((LongPointData)vtmp.Value).Y);
                                break;
                            case (byte)TagType.ULongPoint:
                                re.Write((long)((ULongPointData)vtmp.Value).X);
                                re.Write((long)((ULongPointData)vtmp.Value).Y);
                                break;
                            case (byte)TagType.LongPoint3:
                                re.Write(((LongPoint3Data)vtmp.Value).X);
                                re.Write(((LongPoint3Data)vtmp.Value).Y);
                                re.Write(((LongPoint3Data)vtmp.Value).Z);
                                break;
                            case (byte)TagType.ULongPoint3:
                                re.Write((long)((ULongPoint3Data)vtmp.Value).X);
                                re.Write((long)((ULongPoint3Data)vtmp.Value).Y);
                                re.Write((long)((ULongPoint3Data)vtmp.Value).Z);
                                break;
                        }
                        re.Write(vtmp.Time.Ticks);
                        re.Write(vtmp.Quality);
                    }

                    re.Write(tmp.Ticks);
                    re.WriteByte((byte)QualityConst.Null);
                }
                else
                {
                    value = service.GetTagValue(vv, out qu, out time, out type);

                    if (value != null)
                    {
                        re.WriteByte(type);
                        switch (type)
                        {
                            case (byte)TagType.Bool:
                                re.Write(Convert.ToByte(value));
                                break;
                            case (byte)TagType.Byte:
                                re.Write(Convert.ToByte(value));
                                break;
                            case (byte)TagType.Short:
                                re.Write(Convert.ToInt16(value));
                                break;
                            case (byte)TagType.UShort:
                                re.Write(Convert.ToUInt16(value));
                                break;
                            case (byte)TagType.Int:
                                re.Write(Convert.ToInt32(value));
                                break;
                            case (byte)TagType.UInt:
                                re.Write(Convert.ToUInt32(value));
                                break;
                            case (byte)TagType.Long:
                            case (byte)TagType.ULong:
                                re.Write(Convert.ToInt64(value));
                                break;
                            case (byte)TagType.Float:
                                re.Write(Convert.ToSingle(value));
                                break;
                            case (byte)TagType.Double:
                                re.Write(Convert.ToDouble(value));
                                break;
                            case (byte)TagType.String:
                                string sval = value.ToString();
                                re.Write(sval);
                                //re.Write(sval.Length);
                                //re.Write(sval, Encoding.Unicode);
                                break;
                            case (byte)TagType.DateTime:
                                re.Write(((DateTime)value).Ticks);
                                break;
                            case (byte)TagType.IntPoint:
                                re.Write(((IntPointData)value).X);
                                re.Write(((IntPointData)value).Y);
                                break;
                            case (byte)TagType.UIntPoint:
                                re.Write((int)((UIntPointData)value).X);
                                re.Write((int)((UIntPointData)value).Y);
                                break;
                            case (byte)TagType.IntPoint3:
                                re.Write(((IntPoint3Data)value).X);
                                re.Write(((IntPoint3Data)value).Y);
                                re.Write(((IntPoint3Data)value).Z);
                                break;
                            case (byte)TagType.UIntPoint3:
                                re.Write((int)((UIntPoint3Data)value).X);
                                re.Write((int)((UIntPoint3Data)value).Y);
                                re.Write((int)((UIntPoint3Data)value).Z);
                                break;
                            case (byte)TagType.LongPoint:
                                re.Write(((LongPointData)value).X);
                                re.Write(((LongPointData)value).Y);
                                break;
                            case (byte)TagType.ULongPoint:
                                re.Write((long)((ULongPointData)value).X);
                                re.Write((long)((ULongPointData)value).Y);
                                break;
                            case (byte)TagType.LongPoint3:
                                re.Write(((LongPoint3Data)value).X);
                                re.Write(((LongPoint3Data)value).Y);
                                re.Write(((LongPoint3Data)value).Z);
                                break;
                            case (byte)TagType.ULongPoint3:
                                re.Write((long)((ULongPoint3Data)value).X);
                                re.Write((long)((ULongPoint3Data)value).Y);
                                re.Write((long)((ULongPoint3Data)value).Z);
                                break;
                            case (byte)TagType.Complex:
                                break;
                        }

                        re.Write(time.Ticks);
                        re.WriteByte(qu);
                    }
                    else
                    {
                        re.WriteByte((byte)TagType.Byte);
                        re.WriteByte(0);
                        re.Write(tmp.Ticks);
                        re.WriteByte((byte)QualityConst.Null);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessQueryBySQL(string clientid, ByteBuffer block)
        {
            try
            {
                string sql = block.ReadString();
                int id = block.ReadInt();
                if (!string.IsNullOrEmpty(sql))
                {
                    var sqlexp = ParseSql(sql, out List<int> selids, out Dictionary<int, byte> tps);
                    if(sqlexp.Where == null ||(sqlexp.Where.LowerTime==null&&sqlexp.Where.UpperTime==null))
                    {
                        var re = ProcessQueryRealValueBySql(selids,id);
                        Parent.AsyncCallback(clientid, re);
                        return;
                    }
                    else if(sqlexp.Where.UpperTime==null)
                    {
                        sqlexp.Where.UpperTime = new LowerEqualAction() { IgnorFit = true, Target = DateTime.Now.ToString() };
                    }
                    else if(sqlexp.Where.LowerTime==null)
                    {
                        LoggerService.Service.Warn("DirectAccess", $"Sql 语句格式不支持.");
                        return;
                    }
                    var qq = ServiceLocator.Locator.Resolve<IHisQuery>().ReadAllValueAndFilter(selids,DateTime.Parse(sqlexp.Where.LowerTime.Target.ToString()) , DateTime.Parse(sqlexp.Where.UpperTime.Target.ToString()), sqlexp.Where, tps);
                    if(qq!=null)
                    {
                        if(sqlexp.Select.IsAllNone())
                        {
                            var smetas = qq.SeriseMeta();
                            var re = Parent.Allocate(APIConst.ReadTagHisValueReturn, 12 + qq.AvaiableSize+smetas.Length*2);
                            re.Write(id);
                            re.Write((byte)APIConst.ReadHisValueBySQL);
                            re.Write((byte)0);
                            re.Write(smetas);
                            re.Write(qq.AvaiableSize);
                            re.Write(qq.Address,qq.AvaiableSize);
                            Parent.AsyncCallback(clientid,re);
                            //直接返回表格内容
                        }
                        else
                        {
                            //做二次计算值
                            List<object> vals = new List<object>();
                            foreach(var vv in sqlexp.Select.Selects)
                            {
                                vals.Add(vv.Cal(qq));
                            }

                            var re = Parent.Allocate(APIConst.ReadTagHisValueReturn, 10 + 8*vals.Count);
                            re.Write(id);
                            re.Write((byte)APIConst.ReadHisValueBySQL);
                            re.Write((byte)1);
                            re.Write(vals.Count);
                            foreach(var vv in  vals)
                            {
                                re.Write(Convert.ToDouble(vv));
                            }
                            Parent.AsyncCallback(clientid, re);
                        }
                        qq.Dispose();
                    }
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("DirectAccess", ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetAreaHisData2(string clientid, ByteBuffer block)
        {
            int count = block.ReadInt();
            for (int i = 0; i < count; i++)
            {
                ProcessSetAreaHisDataInner(clientid, block);
            }
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisData2(string clientid, ByteBuffer block)
        {
            //if (CheckBlock(clientid)) return;

            int count = block.ReadInt();
            int scount = 0;
            for (int i = 0; i < count; i++)
            {
                ProcessSetHisDataInner(clientid, block,out int spanCount);
                scount += spanCount;
                //限制写入速度，防止写入过快
                if(scount>12* count)
                {
                    scount = 0;
                    Thread.Sleep(2000);
                }
            }
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        private void ProcessSetHisDataWithTagName(string clientid,ByteBuffer block, out int spanCount)
        {
            var id = block.ReadString();
            var count = block.ReadInt();
            var typ = block.ReadByte();
            //int timedu = block.ReadInt();
            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();
            var realservice = ServiceLocator.Locator.Resolve<IRealTagProduct>();
            int vid = 0;
            if (!Driver.AllowTagNames.ContainsKey(id))
            {
                spanCount = 0;
                return;
            }
            else
            {
                vid = Driver.AllowTagNames[id];
                if (!Driver.DriverdRecordTags.Contains(vid))
                {
                    spanCount = 0;
                    return;
                }
            }

            DateTime mLastTime = DateTime.MinValue;
            int mSpanCount = 0;
            //List<TagValue> tagvalues = new List<TagValue>();
            switch (typ)
            {
                case (byte)TagType.Bool:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();

                        service.SetTagHisValue(vid, dt, bval, qa);

                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.Byte:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);

                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.Short:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadShort();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);

                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.UShort:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadUShort();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);

                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.Int:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadInt();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.UInt:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadUInt();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.Long:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadLong();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.ULong:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = (ulong)block.ReadLong();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.Float:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadFloat();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.Double:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadDouble();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.String:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadString();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.DateTime:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = DateTime.FromBinary(block.ReadLong());
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, bval, qa);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            realservice.SetTagValue(vid, ref bval, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.IntPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval1 = block.ReadInt();
                        var bval2 = block.ReadInt();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, qa, bval1, bval2);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            IntPointData ipd = new IntPointData(bval1, bval2);
                            realservice.SetTagValue(vid, ref ipd, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.UIntPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        //var bval = new UIntPointData() { X = block.ReadUnsignedInt(), Y = block.ReadUnsignedInt() };
                        var bval1 = block.ReadUInt();
                        var bval2 = block.ReadUInt();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, qa, bval1, bval2);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            UIntPointData ipd = new UIntPointData(bval1, bval2);
                            realservice.SetTagValue(vid, ref ipd, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.IntPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        // var bval = new IntPoint3Data() { X = block.ReadInt(), Y = block.ReadInt(),Z=block.ReadInt() };
                        var bval1 = block.ReadInt();
                        var bval2 = block.ReadInt();
                        var bval3 = block.ReadInt();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, qa, bval1, bval2, bval3);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            IntPoint3Data ipd = new IntPoint3Data(bval1, bval2, bval3);
                            realservice.SetTagValue(vid, ref ipd, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.UIntPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        // var bval = new UIntPoint3Data() { X = block.ReadUnsignedInt(), Y = block.ReadUnsignedInt(), Z = block.ReadUnsignedInt() };
                        var bval1 = block.ReadUInt();
                        var bval2 = block.ReadUInt();
                        var bval3 = block.ReadUInt();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, qa, bval1, bval2, bval3);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            UIntPoint3Data ipd = new UIntPoint3Data(bval1, bval2, bval3);
                            realservice.SetTagValue(vid, ref ipd, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.LongPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        // var bval = new LongPointData() { X = block.ReadLong(), Y = block.ReadLong() };
                        var bval1 = block.ReadLong();
                        var bval2 = block.ReadLong();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, qa, bval1, bval2);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            LongPointData ipd = new LongPointData(bval1, bval2);
                            realservice.SetTagValue(vid, ref ipd, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.ULongPoint:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        //  var bval = new ULongPointData() { X = (ulong)block.ReadLong(), Y = (ulong)block.ReadLong() };
                        var bval1 = (ulong)block.ReadLong();
                        var bval2 = (ulong)block.ReadLong();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, qa, bval1, bval2);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            ULongPointData ipd = new ULongPointData(bval1, bval2);
                            realservice.SetTagValue(vid, ref bval1, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.LongPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        // var bval = new LongPoint3Data() { X = block.ReadLong(), Y = block.ReadLong(),Z=block.ReadLong() };
                        var bval1 = block.ReadLong();
                        var bval2 = block.ReadLong();
                        var bval3 = block.ReadLong();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, qa, bval1, bval2, bval3);
                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            LongPoint3Data ipd = new LongPoint3Data(bval1, bval2, bval3);
                            realservice.SetTagValue(vid, ref ipd, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
                case (byte)TagType.ULongPoint3:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        //  var bval = new ULongPoint3Data() { X = (ulong)block.ReadLong(), Y = (ulong)block.ReadLong(), Z = (ulong)block.ReadLong() };
                        var bval1 = (ulong)block.ReadLong();
                        var bval2 = (ulong)block.ReadLong();
                        var bval3 = (ulong)block.ReadLong();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(vid, dt, qa, bval1, bval2, bval3);

                        //将最后一个值写入到实时值
                        if (i == count - 1)
                        {
                            ULongPoint3Data ipd = new ULongPoint3Data(bval1, bval2, bval3);
                            realservice.SetTagValue(vid, ref ipd, qa);
                        }
                        CheckAndDelay(dt, ref mLastTime, ref mSpanCount);
                    }
                    break;
            }
            spanCount = mSpanCount;
            service?.SubmitCach();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisDataWithUser(string clientid, ByteBuffer block)
        {
            //if (CheckBlock(clientid)) return;

            ProcessSetHisDataWithTagName(clientid, block,out int spanCount);
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        //private bool CheckBlock(string clientid)
        //{
        //    DateTime dt =DateTime.Now;
        //    while (ServiceLocator.Locator.Resolve<IDataCompressService>().IsBlocked())
        //    {
        //        Thread.Sleep(1000);
        //        if((DateTime.Now - dt).TotalSeconds>5)
        //        {
        //            LoggerService.Service.Warn("DirectAccess",$" 系统资源不足导致 {clientid} 写入历史数据失败！");
        //            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)10));
        //            return true;
        //        }
        //    }
        //    return false;
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisData2WithUser(string clientid, ByteBuffer block)
        {
            //if (CheckBlock(clientid)) return;

            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();
            int count = block.ReadInt();
            int scount = 0;
            for (int i = 0; i < count; i++)
            {
                ProcessSetHisDataWithTagName(clientid, block, out int spanCount);
                scount += spanCount;
                //限制写入速度，防止写入过快
                if (scount > 12 * count)
                {
                    scount = 0;
                    Thread.Sleep(1000);
                }
            }
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        #region Read His Value

        /// <summary>
        /// 读取变量的所有值
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private unsafe void ProcessRequestAllHisDataByMemory(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            DateTime sTime = new DateTime(data.ReadLong());
            DateTime eTime = new DateTime(data.ReadLong());
            int cid = data.ReadInt();
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            ByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<bool>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<byte>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<DateTime>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<double>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<float>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<int>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<long>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<short>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<string>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<bool>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ulong>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ushort>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPointData>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, sTime, eTime));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, sTime, eTime));
                        break;
                }
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                re = Parent.Allocate(APIConst.ReadTagHisValueReturn, 5 + 4);
                re.Write(cid);
                re.Write(0);
                re.Write(0);
                Parent.AsyncCallback(clientId, re);
                //Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestHisDatasByTimePoint(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            int count = data.ReadInt();
            List<DateTime> times = new List<DateTime>();
            for (int i = 0; i < count; i++)
            {
                times.Add(new DateTime(data.ReadLong()));
            }
            int cid = data.ReadInt();
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            ByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, times, type));
                        break;
                }
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                //Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
                re = Parent.Allocate(APIConst.ReadTagHisValueReturn, 5 + 4);
                re.Write(cid);
                re.Write(0);
                re.Write(0);
                Parent.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestHisDatasByTimePointByIgnoreClosedQualtiy(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            int count = data.ReadInt();
            List<DateTime> times = new List<DateTime>();
            for (int i = 0; i < count; i++)
            {
                times.Add(new DateTime(data.ReadLong()));
            }
            int cid = data.ReadInt();
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            ByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ULongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ULongPoint3Data>(id, times, type));
                        break;
                }
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                //Parent.AsyncCallback(clientId, FunId, new byte[1], 0);
                re = Parent.Allocate(APIConst.ReadTagHisValueReturn, 5 + 4);
                re.Write(cid);
                re.Write(0);
                re.Write(0);
                Parent.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestHisDataByTimeSpanByMemory(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            DateTime stime = new DateTime(data.ReadLong());
            DateTime etime = new DateTime(data.ReadLong());
            TimeSpan ts = new TimeSpan(data.ReadLong());
            List<DateTime> times = new List<DateTime>();
            DateTime tmp = stime;
            while (tmp <= etime)
            {
                times.Add(tmp);
                tmp += ts;
            }
            int cid = data.ReadInt();
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            ByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<uint>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQuery<ULongPoint3Data>(id, times, type));
                        break;
                }
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                re = Parent.Allocate(APIConst.ReadTagHisValueReturn, 5 + 4);
                re.Write(cid);
                re.Write(0);
                re.Write(0);
                Parent.AsyncCallback(clientId, re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="data"></param>
        private void ProcessRequestHisDataByTimeSpanByMemoryByIgnorClosedQuality(string clientId, ByteBuffer data)
        {
            int id = data.ReadInt();
            Cdy.Tag.QueryValueMatchType type = (QueryValueMatchType)data.ReadByte();
            DateTime stime = new DateTime(data.ReadLong());
            DateTime etime = new DateTime(data.ReadLong());
            TimeSpan ts = new TimeSpan(data.ReadLong());
            List<DateTime> times = new List<DateTime>();
            DateTime tmp = stime;
            while (tmp <= etime)
            {
                times.Add(tmp);
                tmp += ts;
            }
            int cid = data.ReadInt();
            var tags = ServiceLocator.Locator.Resolve<ITagManager>().GetTagById(id);

            ByteBuffer re = null;

            if (tags != null)
            {
                switch (tags.Type)
                {
                    case Cdy.Tag.TagType.Bool:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<bool>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Byte:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<byte>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.DateTime:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<DateTime>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Double:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<double>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Float:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<float>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Int:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<int>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Long:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<long>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.Short:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<short>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.String:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<string>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UInt:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<uint>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULong:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ulong>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UShort:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ushort>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<IntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<UIntPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.IntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<IntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.UIntPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<UIntPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<LongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ULongPointData>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.LongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<LongPoint3Data>(id, times, type));
                        break;
                    case Cdy.Tag.TagType.ULongPoint3:
                        re = WriteDataToBufferByMemory(cid, (byte)tags.Type, ProcessDataQueryByIgnorClosedQuality<ULongPoint3Data>(id, times, type));
                        break;
                }
                Parent.AsyncCallback(clientId, re);
            }
            else
            {
                re = Parent.Allocate(APIConst.ReadTagHisValueReturn, 5 + 4);
                re.Write(cid);
                re.Write(0);
                re.Write(0);
                Parent.AsyncCallback(clientId, re);
            }
        }


        private unsafe ByteBuffer WriteDataToBufferByMemory<T>(int cid, byte type, HisQueryResult<T> resb)
        {
            var vdata = resb.Contracts();
            //var vdata = resb;
            var re = Parent.Allocate(APIConst.ReadTagHisValueReturn, 5 + vdata.Size + 4);
            re.Write(cid);
            re.Write(type);
            re.Write(resb.Count);
            re.Write(vdata.Address, vdata.Size);

            resb.Dispose();
            vdata.Dispose();
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="stime"></param>
        /// <param name="etime"></param>
        /// <returns></returns>
        private HisQueryResult<T> ProcessDataQuery<T>(int id, DateTime stime, DateTime etime)
        {
            return ServiceLocator.Locator.Resolve<IHisQuery>().ReadAllValueByUTCTime<T>(id, stime, etime);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        private HisQueryResult<T> ProcessDataQuery<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType type)
        {
            return ServiceLocator.Locator.Resolve<IHisQuery>().ReadValueByUTCTime<T>(id, times, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        private HisQueryResult<T> ProcessDataQueryByIgnorClosedQuality<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType type)
        {
            return ServiceLocator.Locator.Resolve<IHisQuery>().ReadValueIgnorClosedQuality<T>(id, times, type);
        }
        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
