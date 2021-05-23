//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:00:38.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Cdy.Tag;
using Cdy.Tag.Common;
using Cdy.Tag.Driver;
using Cheetah;

namespace SpiderDriver
{
    /// <summary>
    /// 
    /// </summary>
    public class RealDataServerProcess : ServerProcessBase
    {

        #region ... Variables  ...
        

        private Dictionary<string, IdBuffer> mCallBackRegistorIds = new Dictionary<string, IdBuffer>();


        private Queue<KeyValuePair<int,object>> mChangedTags = new Queue<KeyValuePair<int, object>>(10);


        private Thread mScanThread;

        private ManualResetEvent resetEvent;

        private bool mIsClosed = false;

        private Dictionary<string, ByteBuffer> buffers = new Dictionary<string, ByteBuffer>();

        private Dictionary<string, int> mDataCounts = new Dictionary<string, int>();

        private ITagManager mTagManager;

        ///// <summary>
        ///// 
        ///// </summary>
        //public static HashSet<int> Driver.AllowTagIds = new HashSet<int>();

        private bool mIsBusy = false;

        private string mName;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public RealDataServerProcess()
        {
            mTagManager = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.ITagManager>();
        }

        #endregion ...Constructor...

        #region ... Properties ...

        public override byte FunId => APIConst.RealValueFun;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            mName = "SpiderDriver" + Guid.NewGuid().ToString();
            ServiceLocator.Locator.Resolve<IRealTagProduct>().SubscribeValueChangedForProducter(mName,
           new ProducterValueChangedNotifyProcesser.ValueChangedDelagete((vals) =>
           {
               lock (mChangedTags)
               {
                   foreach (var vv in vals)
                   {
                       mChangedTags.Enqueue(vv);
                   }
               }
               //mChangedTags.Enqueue(vals);
               resetEvent.Set();
           }), () => { return new List<int>() { -1 }; });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        public override void CheckDataBusy(string client)
        {
            //if (mDatasCach.ContainsKey(client) && mDatasCach[client].Count > 100)
            //{
            //    Parent.AsyncCallback(client, ToByteBuffer(APIConst.AysncReturn, APIConst.RealServerBusy));
            //    mIsBusy = true;
            //}
            //else if(mIsBusy)
            //{
            //    mIsBusy = false;
            //    Parent.AsyncCallback(client, ToByteBuffer(APIConst.AysncReturn, APIConst.RealServerNoBusy));
            //}
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected  override void ProcessSingleData(string client, ByteBuffer data)
        {
            if(data.RefCount==0)
            {
                Debug.Print("invailed data buffer in RealDataServerProcess");
                return;
            }
          
            byte cmd = data.ReadByte();
            long id = data.ReadLong();
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            //LoggerService.Service.Info("RealDataServer","Real Data Server Process:" + cmd);
            if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(id))
            {
                try
                {
                    switch (cmd)
                    {
                        case APIConst.SetTagValueFun:
                            ProcessSetRealData(client, data);
                            break;
                        case APIConst.SetTagValueAndQualityFun:
                            ProcessSetRealDataAndQuality(client, data);
                            break;
                        case APIConst.SetTagRealAndHisValueFun:
                            ProcessSetRealAndHistData(client, data);
                            break;
                        case APIConst.RegistorTag:
                            ProcessValueChangeNotify(client, data);
                            break;
                        case APIConst.RemoveRegistorTag:
                            ProcessRemoveValueChangeNotify(client, data);
                            break;
                        case APIConst.ClearRegistorTag:
                            ProcessResetValueChangedNotify(client, data);
                            break;
                    }
                }
                catch(Exception ex)
                {
                    LoggerService.Service.Warn("SpiderDriver", ex.Message);
                }
            }
            else
            {
                Parent.AsyncCallback(client, FunId, new byte[1], 0);
            }
            //sw.Stop();
            //LoggerService.Service.Info("RealDataServer", "finish Real Data Server Process:" + sw.ElapsedMilliseconds);
            base.ProcessSingleData(client, data);
        }
        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetRealData(string clientid, ByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagProduct>();
            Parent?.AsyncCallback(clientid, ToByteBuffer(APIConst.RealValueFun, (byte)1));

            int count = block.ReadInt();
            int id = 0;
            byte typ;
            for (int i = 0; i < count; i++)
            {
                id = block.ReadInt();
                typ = block.ReadByte();
               
                switch (typ)
                {
                    case (byte)TagType.Bool:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadByte();
                            service.SetTagValue(id,ref bval, 0);
                        }
                        else
                        {
                            block.ReadByte();
                        }
                        break;
                    case (byte)TagType.Byte:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadByte();
                            service.SetTagValue(id,ref bval, 0);
                        }
                        else
                        {
                            block.ReadByte();
                        }
                        break;
                    case (byte)TagType.Short:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadShort();
                            service.SetTagValue(id,ref bval, 0);
                        }
                        else
                        {
                            block.ReadShort();
                        }
                        //value = block.ReadShort();
                        break;
                    case (byte)TagType.UShort:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = (ushort)block.ReadShort();
                            service.SetTagValue(id,ref bval, 0);
                        }
                        else
                        {
                            block.ReadShort();
                        }
                        //value = (ushort)block.ReadShort();
                        break;
                    case (byte)TagType.Int:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadInt();
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadInt();
                        }
                        //value = block.ReadInt();
                        break;
                    case (byte)TagType.UInt:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = (uint)block.ReadInt();
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadInt();
                        }
                        //value = (uint)block.ReadInt();
                        break;
                    case (byte)TagType.Long:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadLong();
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadLong();
                        }
                        //value = block.ReadLong();
                        break;
                    case (byte)TagType.ULong:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = (ulong)block.ReadLong();
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadLong();
                        }
                        //value = (ulong)block.ReadLong();
                        break;
                    case (byte)TagType.Float:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadFloat();
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadFloat();
                        }
                       // value = block.ReadFloat();
                        break;
                    case (byte)TagType.Double:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadDouble();
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadDouble();
                        }
                       // block.ReadDouble();
                        break;
                    case (byte)TagType.String:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            service.SetTagValue(id, block.ReadString(), 0);
                        }
                        else
                        {
                            block.ReadString();
                        }
                        //value = block.ReadString();
                        break;
                    case (byte)TagType.DateTime:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = DateTime.FromBinary(block.ReadLong());
                            service.SetTagValue(id,ref bval, 0);
                        }
                        else
                        {
                            block.ReadLong();
                        }
                        //value = DateTime.FromBinary(block.ReadLong());
                        break;
                    case (byte)TagType.IntPoint:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new IntPointData(block.ReadInt(), block.ReadInt());
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadInt();
                            block.ReadInt();
                        }
                        //value = new IntPointData(block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.UIntPoint:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new UIntPointData(block.ReadInt(), block.ReadInt());
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadInt();
                            block.ReadInt();
                        }
                        //value = new UIntPointData(block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.IntPoint3:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadInt();
                            block.ReadInt();
                            block.ReadInt();
                        }
                        //value = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.UIntPoint3:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadInt();
                            block.ReadInt();
                            block.ReadInt();
                        }
                        //value = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.LongPoint:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new LongPointData(block.ReadLong(), block.ReadLong());
                            service.SetTagValue(id,ref bval, 0);
                        }
                        else
                        {
                            block.ReadLong();
                            block.ReadLong();
                        }
                        //value = new LongPointData(block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.ULongPoint:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new ULongPointData(block.ReadLong(), block.ReadLong());
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadLong();
                            block.ReadLong();
                        }
                        //value = new ULongPointData(block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.LongPoint3:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadLong();
                            block.ReadLong();
                            block.ReadLong();
                        }
                        //value = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.ULongPoint3:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                            service.SetTagValue(id, ref bval, 0);
                        }
                        else
                        {
                            block.ReadLong();
                            block.ReadLong();
                            block.ReadLong();
                        }
                        //value = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                        break;
                }
            }
            service?.SubmiteNotifyChanged();
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetRealAndHistData(string clientid, ByteBuffer block)
        {
            var service = ServiceLocator.Locator.Resolve<IRealTagProduct>();
            var hisservice = ServiceLocator.Locator.Resolve<ITagHisValueProduct>();
            Parent?.AsyncCallback(clientid, ToByteBuffer(APIConst.RealValueFun, (byte)1));

            int count = block.ReadInt();
            int id = 0;
            byte typ;
            byte qua = 0;
            for (int i = 0; i < count; i++)
            {
                id = block.ReadInt();
                typ = block.ReadByte();

                switch (typ)
                {
                    case (byte)TagType.Bool:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadByte();
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);

                            hisservice.SetTagHisValue<bool>(id, bval>0,qua);
                        }
                        else
                        {
                            block.ReadByte();
                            qua = block.ReadByte();
                        }
                        break;
                    case (byte)TagType.Byte:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadByte();
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);

                            hisservice.SetTagHisValue<byte>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadByte();
                            qua = block.ReadByte();
                        }
                        break;
                    case (byte)TagType.Short:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadShort();
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);

                            hisservice.SetTagHisValue<short>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadShort();
                            qua = block.ReadByte();
                        }
                        //value = block.ReadShort();
                        break;
                    case (byte)TagType.UShort:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = (ushort)block.ReadShort();
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<ushort>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadShort();
                            qua = block.ReadByte();
                        }
                        //value = (ushort)block.ReadShort();
                        break;
                    case (byte)TagType.Int:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadInt();
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<int>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadInt();
                            qua = block.ReadByte();
                        }
                        //value = block.ReadInt();
                        break;
                    case (byte)TagType.UInt:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = (uint)block.ReadInt();
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<uint>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadInt();
                            qua = block.ReadByte();
                        }
                        //value = (uint)block.ReadInt();
                        break;
                    case (byte)TagType.Long:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadLong();
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<long>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadLong();
                            qua = block.ReadByte();
                        }
                        //value = block.ReadLong();
                        break;
                    case (byte)TagType.ULong:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = (ulong)block.ReadLong();
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<ulong>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadLong();
                            qua = block.ReadByte();
                        }
                        //value = (ulong)block.ReadLong();
                        break;
                    case (byte)TagType.Float:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadFloat();
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<float>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadFloat();
                            qua = block.ReadByte();
                        }
                        // value = block.ReadFloat();
                        break;
                    case (byte)TagType.Double:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = block.ReadDouble();
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<double>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadDouble();
                            qua = block.ReadByte();
                        }
                        // block.ReadDouble();
                        break;
                    case (byte)TagType.String:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bs = block.ReadString();
                            qua = block.ReadByte();
                            service.SetTagValue(id, bs, qua);
                            hisservice.SetTagHisValue<string>(id, bs, qua);
                        }
                        else
                        {
                            block.ReadString();
                            qua = block.ReadByte();
                        }
                        //value = block.ReadString();
                        break;
                    case (byte)TagType.DateTime:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = DateTime.FromBinary(block.ReadLong());
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<DateTime>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadLong();
                            qua = block.ReadByte();
                        }
                        //value = DateTime.FromBinary(block.ReadLong());
                        break;
                    case (byte)TagType.IntPoint:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new IntPointData(block.ReadInt(), block.ReadInt());
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<IntPointData>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadInt();
                            block.ReadInt();
                            qua = block.ReadByte();
                        }
                        //value = new IntPointData(block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.UIntPoint:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new UIntPointData(block.ReadInt(), block.ReadInt());
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<UIntPointData>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadInt();
                            block.ReadInt();
                            qua = block.ReadByte();
                        }
                        //value = new UIntPointData(block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.IntPoint3:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<IntPoint3Data>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadInt();
                            block.ReadInt();
                            block.ReadInt();
                            qua = block.ReadByte();
                        }
                        //value = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.UIntPoint3:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<UIntPoint3Data>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadInt();
                            block.ReadInt();
                            block.ReadInt();
                            qua = block.ReadByte();
                        }
                        //value = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.LongPoint:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new LongPointData(block.ReadLong(), block.ReadLong());
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<LongPointData>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadLong();
                            block.ReadLong();
                            qua = block.ReadByte();
                        }
                        //value = new LongPointData(block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.ULongPoint:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new ULongPointData(block.ReadLong(), block.ReadLong());
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<ULongPointData>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadLong();
                            block.ReadLong();
                            qua = block.ReadByte();
                        }
                        //value = new ULongPointData(block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.LongPoint3:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<LongPoint3Data>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadLong();
                            block.ReadLong();
                            block.ReadLong();
                            qua = block.ReadByte();
                        }
                        //value = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.ULongPoint3:
                        if (Driver.AllowTagIds.Contains(id))
                        {
                            var bval = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                            qua = block.ReadByte();
                            service.SetTagValue(id, ref bval, qua);
                            hisservice.SetTagHisValue<ULongPoint3Data>(id, bval, qua);
                        }
                        else
                        {
                            block.ReadLong();
                            block.ReadLong();
                            block.ReadLong();
                            qua = block.ReadByte();
                        }
                        //value = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                        break;
                }
                
            }
            service?.SubmiteNotifyChanged();
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientid"></param>
        /// <param name="block"></param>
        private void ProcessSetRealDataAndQuality(string clientid, ByteBuffer block)
        {
            Parent?.AsyncCallback(clientid, ToByteBuffer(APIConst.RealValueFun, (byte)1));
            var service = ServiceLocator.Locator.Resolve<IRealTagProduct>();
            int count = block.ReadInt();
            byte qua;
            for (int i = 0; i < count; i++)
            {
                var id = block.ReadInt();
                byte typ = block.ReadByte();
                switch (typ)
                {
                    case (byte)TagType.Bool:
                        var bvalue = block.ReadByte();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref bvalue, qua);

                        break;
                    case (byte)TagType.Byte:
                        var  bbvalue = block.ReadByte();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref bbvalue, qua);

                        break;
                    case (byte)TagType.Short:
                        var svalue = block.ReadShort();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref svalue, qua);
                        break;
                    case (byte)TagType.UShort:
                        var  uvalue = (ushort)block.ReadShort();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref uvalue, qua);
                        break;
                    case (byte)TagType.Int:
                        var ivalue = block.ReadInt();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref ivalue, qua);
                        break;
                    case (byte)TagType.UInt:
                        var uivalue = (uint)block.ReadInt();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref uivalue, qua);
                        break;
                    case (byte)TagType.Long:
                        var lvalue = block.ReadLong();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref lvalue, qua);
                        break;
                    case (byte)TagType.ULong:
                        var ulvalue = (ulong)block.ReadLong();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref ulvalue, qua);
                        break;
                    case (byte)TagType.Float:
                        var fvalue = block.ReadFloat();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref fvalue, qua);
                        break;
                    case (byte)TagType.Double:
                        var dvalue = block.ReadDouble();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref dvalue, qua);
                        break;
                    case (byte)TagType.String:
                        var ssvalue = block.ReadString();
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ssvalue, qua);
                        break;
                    case (byte)TagType.DateTime:
                        var tick = block.ReadLong();
                        var ddvalue = new DateTime(tick);
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref ddvalue, qua);
                        break;
                    case (byte)TagType.IntPoint:
                        var ipvalue = new IntPointData(block.ReadInt(), block.ReadInt());
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref ipvalue, qua);
                        break;
                    case (byte)TagType.UIntPoint:
                        var uipvalue = new UIntPointData(block.ReadInt(), block.ReadInt());
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref uipvalue, qua);
                        break;
                    case (byte)TagType.IntPoint3:
                        var ip3value = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref ip3value, qua);
                        break;
                    case (byte)TagType.UIntPoint3:
                        var uip3value = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref uip3value, qua);
                        break;
                    case (byte)TagType.LongPoint:
                       var  lpvalue = new LongPointData(block.ReadLong(), block.ReadLong());
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref lpvalue, qua);
                        break;
                    case (byte)TagType.ULongPoint:
                       var upvalue = new ULongPointData(block.ReadLong(), block.ReadLong());
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref upvalue, qua);
                        break;
                    case (byte)TagType.LongPoint3:
                       var lp3value = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref lp3value, qua);
                        break;
                    case (byte)TagType.ULongPoint3:
                       var up3value = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                        qua = block.ReadByte();
                        if (Driver.AllowTagIds.Contains(id))
                            service.SetTagValue(id, ref up3value, qua);
                        break;
                }
            }
            service?.SubmiteNotifyChanged();
            
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="clientId"></param>
        /// <param name="block"></param>
        private void ProcessRemoveValueChangeNotify(string clientId, ByteBuffer block)
        {
            try
            {
                if (mCallBackRegistorIds.ContainsKey(clientId))
                {
                    var itmp = mCallBackRegistorIds[clientId];
                    int minid = block.ReadInt();
                    for (int i = 0; i < minid; i++)
                    {
                        var ival = block.ReadInt();
                        itmp.ClearId(ival);
                    }
                }
                Parent?.AsyncCallback(clientId, ToByteBuffer(APIConst.RealValueFun, (byte)1));
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessValueChangeNotify(string clientId, ByteBuffer block)
        {
            try
            {
                int minid = block.ReadInt();
                
                for (int i = 0; i < minid; i++)
                {
                    var vv = block.ReadInt();
                    if (Driver.AllowTagIds.Contains(vv))
                    {
                        if (mCallBackRegistorIds.ContainsKey(clientId))
                        {
                            var itmp = mCallBackRegistorIds[clientId];
                            itmp.SetId(vv);
                        }
                        else
                        {
                            IdBuffer ids = new IdBuffer();
                            mCallBackRegistorIds.Add(clientId, ids);
                            ids.SetId(vv);
                        }
                    }
                }

                

                if (!mDataCounts.ContainsKey(clientId))
                {
                    mDataCounts.Add(clientId, 0);
                }

                Parent?.AsyncCallback(clientId, ToByteBuffer(APIConst.RealValueFun, (byte)1));
            }
            catch(Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void ProcessResetValueChangedNotify(string clientId, ByteBuffer block)
        {
            try
            {
                if (mCallBackRegistorIds.ContainsKey(clientId))
                {
                    mCallBackRegistorIds.Remove(clientId);
                }
                if (mDataCounts.ContainsKey(clientId))
                {
                    mDataCounts.Remove(clientId);
                }
            }
            catch
            {

            }
            Parent?.AsyncCallback(clientId, ToByteBuffer(APIConst.RealValueFun, (byte)1));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="id"></param>
        private void ProcessTagPush(ByteBuffer re,int id,byte type,object value)
        {
            re.Write(id);
            re.Write(type);
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
                    //re.Write(sval.Length);
                    re.Write(sval, Encoding.Unicode);
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
            }
        }



        /// <summary>
        /// 
        /// </summary>
        private void SendThreadPro()
        {
            while(!mIsClosed)
            {
                try
                {
                    resetEvent.WaitOne();
                    if (mIsClosed) return;
                    resetEvent.Reset();

                    HashSet<int> hval = new HashSet<int>();

                    List<KeyValuePair<int, object>>  changtags = new List<KeyValuePair<int, object>>();

                    lock (mChangedTags)
                    {
                        changtags.AddRange(mChangedTags);
                        mChangedTags.Clear();
                    }

                    if (mCallBackRegistorIds.Count == 0) break;

                    var clients = mCallBackRegistorIds.ToArray();
                    foreach (var cb in clients)
                    {
                        var buffer = Parent.Allocate(APIConst.PushDataChangedFun, changtags.Count * 64 + 5);
                        buffer.Write(0);
                        buffers.Add(cb.Key, buffer);
                        mDataCounts[cb.Key] = 0;
                    }

                    foreach (var vv in changtags)
                    {
                        foreach (var vvc in clients)
                        {
                            try
                            {
                                if (vvc.Value.CheckId(vv.Key))
                                {
                                    byte tp = (byte)mTagManager.GetTagById(vv.Key).Type;
                                    ProcessTagPush(buffers[vvc.Key], vv.Key, tp, vv.Value);
                                    mDataCounts[vvc.Key]++;
                                }
                            }
                            catch (Exception ex)
                            {
                                LoggerService.Service.Erro("SpiderDriver", "RealDataService " + ex.Message);
                            }
                        }
                    }

                    foreach (var cb in buffers)
                    {
                        if (cb.Value.WriteIndex > 6)
                        {
                            var idx = cb.Value.WriteIndex;
                            //cb.Value.MarkWriterIndex();
                            cb.Value.WriteIndex = 1;
                            //cb.Value.SetWriterIndex(1);
                            cb.Value.Write(mDataCounts[cb.Key]);
                            //cb.Value.ResetWriterIndex();
                            cb.Value.WriteIndex = idx;
                        }
                        Parent.PushRealDatatoClient(cb.Key, cb.Value);
                    }
                    buffers.Clear();
                }
                catch(Exception ex)
                {
                    LoggerService.Service.Erro("RealDataServerProcess", ex.StackTrace);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>

        public override void Start()
        {
           
            base.Start();
            resetEvent = new ManualResetEvent(false);
            mScanThread = new Thread(SendThreadPro);
            mScanThread.IsBackground = true;
            mScanThread.Start();
           
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Stop()
        {
            mIsClosed = true;
            resetEvent.Set();
            resetEvent.Close();
            base.Stop();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public override void OnClientConnected(string id)
        {
            if(!mCallBackRegistorIds.ContainsKey(id))
            {
                mCallBackRegistorIds.Add(id,new IdBuffer());
            }
            base.OnClientConnected(id);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public override void OnClientDisconnected(string id)
        {
            if(mCallBackRegistorIds.ContainsKey(id))
            {
                mCallBackRegistorIds[id].Dispose();
                mCallBackRegistorIds.Remove(id);
            }
            base.OnClientDisconnected(id);
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Dispose()
        {
            ServiceLocator.Locator.Resolve<IRealTagProduct>().UnSubscribeValueChangedForProducter(mName);
            this.mTagManager = null;
            base.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
