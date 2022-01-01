//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/9/23 15:44:01.
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

namespace SpiderDriver
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
                LoggerService.Service.Warn("SpiderDriver_HisDataServerProcess", "invailed data buffer in HisDataServerProcess");
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
                        LoggerService.Service.Warn("Spider HisDataServerProcess", ex.Message);
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
                        LoggerService.Service.Warn("Spider HisDataServerProcess", ex.Message);
                    }
                }
            }
            base.ProcessSingleData(client, data);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisData(string clientid, ByteBuffer block)
        {
            var id = block.ReadInt();
            var count = block.ReadInt();
            var typ = block.ReadByte();
            //int timedu = block.ReadInt();
            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();

            //List<TagValue> tagvalues = new List<TagValue>();
            switch (typ)
            {
                case (byte)TagType.Bool:
                    for(int i=0;i<count;i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, bval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Byte:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();
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
                        service.SetTagHisValue(id, dt, qa,bval1,bval2);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
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
                        service.SetTagHisValue(id, dt,qa,bval1,bval2,bval3);
                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
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
                        service.SetTagHisValue(id, dt,  qa,bval1,bval2,bval3);
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
                        service.SetTagHisValue(id, dt,  qa,bval1,bval2);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
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
                        service.SetTagHisValue(id, dt, qa, bval1, bval2,bval3);
                    }
                    break;
            }
            
            
            //service.SetTagHisValue(id, tagvalues);

            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisDataWithUser(string clientid, ByteBuffer block)
        {
            var id = block.ReadString();
            var count = block.ReadInt();
            var typ = block.ReadByte();
            //int timedu = block.ReadInt();
            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();

            //List<TagValue> tagvalues = new List<TagValue>();
            switch (typ)
            {
                case (byte)TagType.Bool:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Byte:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Short:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadShort();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        //  service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.UShort:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadUShort();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Int:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadInt();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.UInt:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadUInt();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Long:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadLong();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.ULong:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = (ulong)block.ReadLong();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Float:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadFloat();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.Double:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadDouble();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.String:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = block.ReadString();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
                    }
                    break;
                case (byte)TagType.DateTime:
                    for (int i = 0; i < count; i++)
                    {
                        var dt = DateTime.FromBinary(block.ReadLong());
                        var bval = DateTime.FromBinary(block.ReadLong());
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
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
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, bval1, bval2);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
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
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, bval1, bval2);
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
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, bval1, bval2, bval3);
                        // service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
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
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, bval1, bval2, bval3);
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
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, bval1, bval2);
                        //service.SetTagHisValue(id, new TagValue() { Quality = qa, Time = dt, Value = bval });
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
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, bval1, bval2);
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
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, bval1, bval2, bval3);
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
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, bval1, bval2, bval3);
                    }
                    break;
            }


            //service.SetTagHisValue(id, tagvalues);

            //Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetHisData2(string clientid, ByteBuffer block)
        {
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();
            int count = block.ReadInt();
            for (int i = 0; i < count; i++)
            {
                int id = block.ReadInt();
                var dt = DateTime.FromBinary(block.ReadLong());
                TagType tp = (TagType)block.ReadByte();
                switch (tp)
                {
                    case TagType.Bool:
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(id,dt,bval,qa);
                        break;
                    case TagType.Byte:
                        var btval = block.ReadByte();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, btval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = btval });
                        break;
                    case TagType.Short:
                        var sbval = block.ReadShort();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, sbval, qa);
                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = sbval });
                        break;
                    case TagType.UShort:
                        var usbval = block.ReadUShort();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, usbval, qa);
                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = usbval });
                        break;
                    case TagType.Int:
                        var ibval = block.ReadInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, ibval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = ibval });
                        break;
                    case TagType.UInt:
                        var uibval = block.ReadUInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, uibval, qa);
                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = uibval });
                        break;
                    case TagType.Long:
                        var lbval = block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, lbval, qa);
                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = lbval });
                        break;
                    case TagType.ULong:
                        var ulbval = (ulong)block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, ulbval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = ulbval });
                        break;
                    case TagType.Float:
                        var fbval = block.ReadFloat();
                         qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, fbval, qa);

                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = fbval });
                        break;
                    case TagType.Double:
                        var dbval = block.ReadDouble();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, dbval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = dbval });
                        break;
                    case TagType.String:
                        var stbval = block.ReadString();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, stbval, qa);
                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = stbval });
                        break;
                    case TagType.DateTime:
                        var dtbval = DateTime.FromBinary(block.ReadLong());
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, dtbval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = dtbval });
                        break;
                    case TagType.IntPoint:
                        var ival1 = block.ReadInt();
                        var ival2 = block.ReadInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt,  qa, ival1, ival2);
                        break;
                    case TagType.UIntPoint:
                        //var upbval = new UIntPointData() { X = block.ReadUnsignedInt(), Y = block.ReadUnsignedInt() };
                        var uival1 = block.ReadInt();
                        var uival2 = block.ReadInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, qa, uival1, uival2);

                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = upbval });
                        break;
                    case TagType.IntPoint3:
                         ival1 = block.ReadInt();
                         ival2 = block.ReadInt();
                        var ival3 = block.ReadInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, qa, ival1, ival2,ival3);
                        break;
                    case TagType.UIntPoint3:
                        uival1 = block.ReadInt();
                        uival2 = block.ReadInt();
                        var uival3 = block.ReadInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, qa, uival1, uival2,uival3);
                        break;
                    case TagType.LongPoint:
                        var lval1 = block.ReadLong();
                        var lval2 = block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, qa, lval1, lval2);

                        //var lpbval = new LongPointData() { X = block.ReadLong(), Y = block.ReadLong() };
                        //qa = block.ReadByte();
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = lpbval });
                        break;
                    case TagType.ULongPoint:
                        var ulval1 = (ulong)block.ReadLong();
                        var ulval2 = (ulong)block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, qa, ulval1, ulval2);
                        //var ulpbval = new ULongPointData() { X = (ulong)block.ReadLong(), Y = (ulong)block.ReadLong() };
                        //qa = block.ReadByte();
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = ulpbval });
                        break;
                    case TagType.LongPoint3:
                        lval1 = block.ReadLong();
                        lval2 = block.ReadLong();
                        var lval3 = block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, qa, lval1, lval2,lval3);

                        //var lp3bval = new LongPoint3Data() { X = block.ReadLong(), Y = block.ReadLong(), Z = block.ReadLong() };
                        //qa = block.ReadByte();
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = lp3bval });
                        break;
                    case TagType.ULongPoint3:
                        ulval1 = (ulong)block.ReadLong();
                        ulval2 = (ulong)block.ReadLong();
                        var ulval3 = (ulong)block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(id, dt, qa, ulval1, ulval2, ulval3);
                        //var ulp3bval = new ULongPoint3Data() { X = (ulong)block.ReadLong(), Y = (ulong)block.ReadLong(), Z = (ulong)block.ReadLong() };
                        //qa = block.ReadByte();
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = ulp3bval });
                        break;
                }
            }

           
        }


        private void ProcessSetHisData2WithUser(string clientid, ByteBuffer block)
        {
            Parent.AsyncCallback(clientid, ToByteBuffer(APIConst.HisValueFun, (byte)1));
            var service = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();
            int count = block.ReadInt();
            for (int i = 0; i < count; i++)
            {
                var id = block.ReadString();
                var dt = DateTime.FromBinary(block.ReadLong());
                TagType tp = (TagType)block.ReadByte();
                switch (tp)
                {
                    case TagType.Bool:
                        var bval = block.ReadByte();
                        var qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, bval, qa);
                        break;
                    case TagType.Byte:
                        var btval = block.ReadByte();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, btval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = btval });
                        break;
                    case TagType.Short:
                        var sbval = block.ReadShort();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, sbval, qa);
                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = sbval });
                        break;
                    case TagType.UShort:
                        var usbval = block.ReadUShort();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, usbval, qa);
                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = usbval });
                        break;
                    case TagType.Int:
                        var ibval = block.ReadInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, ibval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = ibval });
                        break;
                    case TagType.UInt:
                        var uibval = block.ReadUInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, uibval, qa);
                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = uibval });
                        break;
                    case TagType.Long:
                        var lbval = block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, lbval, qa);
                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = lbval });
                        break;
                    case TagType.ULong:
                        var ulbval = (ulong)block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, ulbval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = ulbval });
                        break;
                    case TagType.Float:
                        var fbval = block.ReadFloat();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, fbval, qa);

                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = fbval });
                        break;
                    case TagType.Double:
                        var dbval = block.ReadDouble();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, dbval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = dbval });
                        break;
                    case TagType.String:
                        var stbval = block.ReadString();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, stbval, qa);
                        // service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = stbval });
                        break;
                    case TagType.DateTime:
                        var dtbval = DateTime.FromBinary(block.ReadLong());
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, dtbval, qa);
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = dtbval });
                        break;
                    case TagType.IntPoint:
                        var ival1 = block.ReadInt();
                        var ival2 = block.ReadInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, ival1, ival2);
                        break;
                    case TagType.UIntPoint:
                        //var upbval = new UIntPointData() { X = block.ReadUnsignedInt(), Y = block.ReadUnsignedInt() };
                        var uival1 = block.ReadInt();
                        var uival2 = block.ReadInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, uival1, uival2);

                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = upbval });
                        break;
                    case TagType.IntPoint3:
                        ival1 = block.ReadInt();
                        ival2 = block.ReadInt();
                        var ival3 = block.ReadInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, ival1, ival2, ival3);
                        break;
                    case TagType.UIntPoint3:
                        uival1 = block.ReadInt();
                        uival2 = block.ReadInt();
                        var uival3 = block.ReadInt();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, uival1, uival2, uival3);
                        break;
                    case TagType.LongPoint:
                        var lval1 = block.ReadLong();
                        var lval2 = block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, lval1, lval2);

                        //var lpbval = new LongPointData() { X = block.ReadLong(), Y = block.ReadLong() };
                        //qa = block.ReadByte();
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = lpbval });
                        break;
                    case TagType.ULongPoint:
                        var ulval1 = (ulong)block.ReadLong();
                        var ulval2 = (ulong)block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, ulval1, ulval2);
                        //var ulpbval = new ULongPointData() { X = (ulong)block.ReadLong(), Y = (ulong)block.ReadLong() };
                        //qa = block.ReadByte();
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = ulpbval });
                        break;
                    case TagType.LongPoint3:
                        lval1 = block.ReadLong();
                        lval2 = block.ReadLong();
                        var lval3 = block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, lval1, lval2, lval3);

                        //var lp3bval = new LongPoint3Data() { X = block.ReadLong(), Y = block.ReadLong(), Z = block.ReadLong() };
                        //qa = block.ReadByte();
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = lp3bval });
                        break;
                    case TagType.ULongPoint3:
                        ulval1 = (ulong)block.ReadLong();
                        ulval2 = (ulong)block.ReadLong();
                        var ulval3 = (ulong)block.ReadLong();
                        qa = block.ReadByte();
                        service.SetTagHisValue(Driver.AllowTagNames[id], dt, qa, ulval1, ulval2, ulval3);
                        //var ulp3bval = new ULongPoint3Data() { X = (ulong)block.ReadLong(), Y = (ulong)block.ReadLong(), Z = (ulong)block.ReadLong() };
                        //qa = block.ReadByte();
                        //service.SetTagHisValue(id,new TagValue() { Quality = qa, Time = dt, Value = ulp3bval });
                        break;
                }
            }


        }
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
