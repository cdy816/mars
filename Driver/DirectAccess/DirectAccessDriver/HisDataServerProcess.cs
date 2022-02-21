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
using System.Text;

namespace DirectAccessDriver
{
    /// <summary>
    /// 
    /// </summary>
    public class HisDataServerProcess : ServerProcessBase
    {

        #region ... Variables  ...
        private bool mIsBusy = false;
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
        protected override void ProcessSingleData(string client, ByteBuffer data)
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
            if (cmd < 100)
            {
                long id = data.ReadLong();
                if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
                {
                    try
                    {
                        switch (cmd)
                        {
                            case APIConst.SetTagHisValue:
                                ProcessSetHisData(client, data);
                                break;
                            case APIConst.SetTagHisValue2:
                                ProcessSetHisData2(client, data);
                                break;
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
                                ProcessSetHisDataWithUser(client, data);
                                break;
                            case APIConst.SetTagHisValueWithUser2:
                                ProcessSetHisData2WithUser(client, data);
                                break;
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

        private void ProcessSetHisDataInner(string clientid,ByteBuffer block)
        {
            var id = block.ReadInt();
            var count = block.ReadInt();
            var typ = block.ReadByte();
            //int timedu = block.ReadInt();
            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();
            var realservice = ServiceLocator.Locator.Resolve<IRealTagProduct>();

            bool ignor = !Driver.DriverdRecordTags.Contains(id);

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
                    }
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisData(string clientid, ByteBuffer block)
        {

            ProcessSetHisDataInner(clientid, block);

            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisData2(string clientid, ByteBuffer block)
        {
            

            int count = block.ReadInt();

            for (int i = 0; i < count; i++)
            {
                ProcessSetHisDataInner(clientid, block);
            }

            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        private void ProcessSetHisDataWithTagName(string clientid,ByteBuffer block)
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
                return;
            }
            else
            {
                vid = Driver.AllowTagNames[id];
                if (!Driver.DriverdRecordTags.Contains(vid)) return;
            }

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
                    }
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisDataWithUser(string clientid, ByteBuffer block)
        {
            ProcessSetHisDataWithTagName(clientid, block);
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisData2WithUser(string clientid, ByteBuffer block)
        {
          
            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();
            int count = block.ReadInt();
            for(int i=0;i<count;i++)
            {
                ProcessSetHisDataWithTagName(clientid, block);
            }
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));

        }
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
