﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Cdy.Tag;
using System.Linq;
using Cheetah;

namespace SpiderDriver.ClientApi
{
    /// <summary>
    /// 
    /// </summary>
    public class DriverProxy:Cdy.Tag.SocketClient2
    {

        #region ... Variables  ...
        
        private long mLoginId;

        private Queue<ByteBuffer> mInfoRequreData = new Queue<ByteBuffer>();

        private ByteBuffer mRealRequreData;

        private ByteBuffer mHisRequreData;

        private ManualResetEvent infoRequreEvent = new ManualResetEvent(false);

        private ManualResetEvent realRequreEvent = new ManualResetEvent(false);

        private ManualResetEvent hisRequreEvent = new ManualResetEvent(false);

        public delegate void ProcessDataPushDelegate(Dictionary<int, object> datas);

        private string mUser, mPass;

        private object mTagInfoLockObj = new object();

        private bool mIsRealDataBusy = false;
        private bool mIsHisDataBusy = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 是否登录
        /// </summary>
        public bool IsLogin { get { return mLoginId > 0; } }

        /// <summary>
        /// 变量的值改变回调
        /// </summary>
        public ProcessDataPushDelegate ValueChanged { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...


        public override void OnConnected(bool isConnected)
        {
            if (!isConnected) mLoginId = -1;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="value"></param>
        //protected override void OnConnectChanged(bool value)
        //{
        //    if (!value) mLoginId = -1;
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="datas"></param>
        protected override void ProcessData(byte fun, ByteBuffer datas)
        {
            
            switch (fun)
            {
                case ApiFunConst.PushDataChangedFun:
                    ValueChanged?.Invoke(ProcessSingleBufferData(datas));
                    break;
                case ApiFunConst.AysncReturn:
                    var fun2= datas.ReadByte();
                    switch (fun2)
                    {
                        case ApiFunConst.RealServerBusy:
                            mIsRealDataBusy = true;
                            break;
                        case ApiFunConst.RealServerNoBusy:
                            mIsRealDataBusy = false;
                            break;
                        case ApiFunConst.HisServerBusy:
                            mIsHisDataBusy = true;
                            break;
                        case ApiFunConst.HisServerNoBusy:
                            mIsHisDataBusy = false;
                            break;
                        case ApiFunConst.TagInfoServerBusy:
                            break;
                        case ApiFunConst.TagINfoServerNoBusy:
                            break;
                    }
                    datas?.UnlockAndReturn();
                    break;
                case ApiFunConst.TagInfoRequestFun:
                    //datas.IncRef();
                    lock (mInfoRequreData)
                        mInfoRequreData.Enqueue(datas);
                    infoRequreEvent.Set();
                    break;
                case ApiFunConst.RealValueFun:
                    //datas.IncRef();
                    if (mRealRequreData != null)
                    {
                        mRealRequreData?.UnlockAndReturn();
                    }
                    mRealRequreData = datas;
                    this.realRequreEvent.Set();
                    break;
                case ApiFunConst.HisValueFun:
                    //datas.IncRef();
                    if (mHisRequreData != null)
                    {
                        mHisRequreData?.UnlockAndReturn();
                    }
                    mHisRequreData = datas;
                    hisRequreEvent.Set();
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        private Dictionary<int, object> ProcessSingleBufferData(ByteBuffer block)
        {
            if (block == null) return null;
            Dictionary<int, object> re = new Dictionary<int, object>();

            var count = block.ReadInt();
            for (int i = 0; i < count; i++)
            {
                var vid = block.ReadInt();
                if (vid < 0)
                {
                    Debug.Print("Invaild value!");
                }
                var typ = block.ReadByte();
                object value = null;
                switch (typ)
                {
                    case (byte)TagType.Bool:
                        value = block.ReadByte();
                        break;
                    case (byte)TagType.Byte:
                        value = block.ReadByte();
                        break;
                    case (byte)TagType.Short:
                        value = block.ReadShort();
                        break;
                    case (byte)TagType.UShort:
                        value = block.ReadUShort();
                        break;
                    case (byte)TagType.Int:
                        value = block.ReadInt();
                        break;
                    case (byte)TagType.UInt:
                        value = block.ReadUInt();
                        break;
                    case (byte)TagType.Long:
                        value = block.ReadLong();
                        break;
                    case (byte)TagType.ULong:
                        value =block.ReadULong();
                        break;
                    case (byte)TagType.Float:
                        value = block.ReadFloat();
                        break;
                    case (byte)TagType.Double:
                        value = block.ReadDouble();
                        break;
                    case (byte)TagType.String:
                        value = block.ReadString();
                        break;
                    case (byte)TagType.DateTime:
                        var tick = block.ReadLong();
                        value = new DateTime(tick);
                        break;
                    case (byte)TagType.IntPoint:
                        value = new IntPointData(block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.UIntPoint:
                        value = new UIntPointData(block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.IntPoint3:
                        value = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.UIntPoint3:
                        value = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                        break;
                    case (byte)TagType.LongPoint:
                        value = new LongPointData(block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.ULongPoint:
                        value = new ULongPointData(block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.LongPoint3:
                        value = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                        break;
                    case (byte)TagType.ULongPoint3:
                        value = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                        break;
                }
                re.Add(vid, value);
            }
           
            block.UnlockAndReturn();
            //block.Release();
            return re;
        }

        /// <summary>
        /// 登录
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="timeount"></param>
        /// <returns>是否成功</returns>
        public bool Login(string username, string password, int timeount = 5000)
        {
             mUser = username;
             mPass = password;
            int size = username.Length + password.Length + 9;
            var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, size);
            mb.Write(ApiFunConst.LoginFun);
            mb.Write(username);
            mb.Write(password);
            SendData(mb);
            if (infoRequreEvent.WaitOne(timeount) && mInfoRequreData.Count>0)
            {
                var vdata = mInfoRequreData.Dequeue();
                try
                {
                    if (vdata.ReadableCount > 4)
                    {
                        mLoginId = vdata.ReadLong();
                        if (mLoginId < 0)
                        {
                            Console.WriteLine("Username or password is invailed!");
                        }
                        return IsLogin;
                    }
                }
                finally
                {
                    vdata?.UnlockAndReturn();
                }
            }
            else
            {
                Console.WriteLine("Login timeout!");
            }
            //mInfoRequreData?.Release();
            mLoginId = -1;
            return IsLogin;
        }

        /// <summary>
        /// 
        /// </summary>
        private void CheckLogin()
        {
            if (mLoginId < 0)
            {
                Login(mUser, mPass);
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="type"></param>
        ///// <param name="value"></param>
        ///// <param name="re"></param>
        //private void SetTagValueToBuffer(TagType type,object value,ByteBuffer re)
        //{
        //    re.Write((byte)type);
        //    switch (type)
        //    {
        //        case TagType.Bool:
        //            re.Write((byte)value);
        //            break;
        //        case TagType.Byte:
        //            re.Write((byte)value);
        //            break;
        //        case TagType.Short:
        //            re.Write((short)value);
        //            break;
        //        case TagType.UShort:
        //            re.Write((ushort)value);
        //            break;
        //        case TagType.Int:
        //            re.Write((int)value);
        //            break;
        //        case TagType.UInt:
        //            re.Write((uint)value);
        //            break;
        //        case TagType.Long:
        //        case TagType.ULong:
        //            re.Write((long)value);
        //            break;
        //        case TagType.Float:
        //            re.Write((float)value);
        //            break;
        //        case TagType.Double:
        //            re.Write((double)value);
        //            break;
        //        case TagType.String:
        //            string sval = value.ToString();
        //            //re.WriteInt(sval.Length);
        //            re.Write(sval, Encoding.Unicode);
        //            break;
        //        case TagType.DateTime:
        //            re.Write(((DateTime)value).Ticks);
        //            break;
        //        case TagType.IntPoint:
        //            re.Write(((IntPointData)value).X);
        //            re.Write(((IntPointData)value).Y);
        //            break;
        //        case TagType.UIntPoint:
        //            re.Write((int)((UIntPointData)value).X);
        //            re.Write((int)((UIntPointData)value).Y);
        //            break;
        //        case TagType.IntPoint3:
        //            re.Write(((IntPoint3Data)value).X);
        //            re.Write(((IntPoint3Data)value).Y);
        //            re.Write(((IntPoint3Data)value).Z);
        //            break;
        //        case TagType.UIntPoint3:
        //            re.Write((int)((UIntPoint3Data)value).X);
        //            re.Write((int)((UIntPoint3Data)value).Y);
        //            re.Write((int)((UIntPoint3Data)value).Z);
        //            break;
        //        case TagType.LongPoint:
        //            re.Write(((LongPointData)value).X);
        //            re.Write(((LongPointData)value).Y);
        //            break;
        //        case TagType.ULongPoint:
        //            re.Write((long)((ULongPointData)value).X);
        //            re.Write((long)((ULongPointData)value).Y);
        //            break;
        //        case TagType.LongPoint3:
        //            re.Write(((LongPoint3Data)value).X);
        //            re.Write(((LongPoint3Data)value).Y);
        //            re.Write(((LongPoint3Data)value).Z);
        //            break;
        //        case TagType.ULongPoint3:
        //            re.Write((long)((ULongPoint3Data)value).X);
        //            re.Write((long)((ULongPoint3Data)value).Y);
        //            re.Write((long)((ULongPoint3Data)value).Z);
        //            break;
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="type"></param>
        ///// <param name="value"></param>
        ///// <param name="re"></param>
        //private void SetTagValueToBuffer2(TagType type, object value, ByteBuffer re)
        //{
        //    switch (type)
        //    {
        //        case TagType.Bool:
        //            re.Write((byte)value);
        //            break;
        //        case TagType.Byte:
        //            re.Write((byte)value);
        //            break;
        //        case TagType.Short:
        //            re.Write((short)value);
        //            break;
        //        case TagType.UShort:
        //            re.Write((ushort)value);
        //            break;
        //        case TagType.Int:
        //            re.Write((int)value);
        //            break;
        //        case TagType.UInt:
        //            re.Write((int)value);
        //            break;
        //        case TagType.Long:
        //        case TagType.ULong:
        //            re.Write((long)value);
        //            break;
        //        case TagType.Float:
        //            re.Write((float)value);
        //            break;
        //        case TagType.Double:
        //            re.Write((double)value);
        //            break;
        //        case TagType.String:
        //            string sval = value.ToString();
        //            //re.WriteInt(sval.Length);
        //            re.Write(sval, Encoding.Unicode);
        //            break;
        //        case TagType.DateTime:
        //            re.Write(((DateTime)value).Ticks);
        //            break;
        //        case TagType.IntPoint:
        //            re.Write(((IntPointData)value).X);
        //            re.Write(((IntPointData)value).Y);
        //            break;
        //        case TagType.UIntPoint:
        //            re.Write((int)((UIntPointData)value).X);
        //            re.Write((int)((UIntPointData)value).Y);
        //            break;
        //        case TagType.IntPoint3:
        //            re.Write(((IntPoint3Data)value).X);
        //            re.Write(((IntPoint3Data)value).Y);
        //            re.Write(((IntPoint3Data)value).Z);
        //            break;
        //        case TagType.UIntPoint3:
        //            re.Write((int)((UIntPoint3Data)value).X);
        //            re.Write((int)((UIntPoint3Data)value).Y);
        //            re.Write((int)((UIntPoint3Data)value).Z);
        //            break;
        //        case TagType.LongPoint:
        //            re.Write(((LongPointData)value).X);
        //            re.Write(((LongPointData)value).Y);
        //            break;
        //        case TagType.ULongPoint:
        //            re.Write((long)((ULongPointData)value).X);
        //            re.Write((long)((ULongPointData)value).Y);
        //            break;
        //        case TagType.LongPoint3:
        //            re.Write(((LongPoint3Data)value).X);
        //            re.Write(((LongPoint3Data)value).Y);
        //            re.Write(((LongPoint3Data)value).Z);
        //            break;
        //        case TagType.ULongPoint3:
        //            re.Write((long)((ULongPoint3Data)value).X);
        //            re.Write((long)((ULongPoint3Data)value).Y);
        //            re.Write((long)((ULongPoint3Data)value).Z);
        //            break;
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="re"></param>
        private void SetTagValueToBuffer(TagType type, object value,byte quality, ByteBuffer re)
        {
            re.Write((byte)type);
            switch (type)
            {
                case TagType.Bool:
                    re.Write(Convert.ToByte(value));
                    break;
                case TagType.Byte:
                    re.Write(Convert.ToByte(value));
                    break;
                case TagType.Short:
                    re.Write(Convert.ToInt16(value));
                    break;
                case TagType.UShort:
                    re.Write(Convert.ToUInt16(value));
                    break;
                case TagType.Int:
                    re.Write(Convert.ToInt32(value));
                    break;
                case TagType.UInt:
                    re.Write(Convert.ToInt32(value));
                    break;
                case TagType.Long:
                case TagType.ULong:
                    re.Write(Convert.ToInt64(value));
                    break;
                case TagType.Float:
                    re.Write(Convert.ToSingle(value));
                    break;
                case TagType.Double:
                    re.Write(Convert.ToDouble(value));
                    break;
                case TagType.String:
                    string sval = value.ToString();
                    //re.WriteInt(sval.Length);
                    re.Write(sval, Encoding.Unicode);
                    break;
                case TagType.DateTime:
                    re.Write(((DateTime)value).ToBinary());
                    break;
                case TagType.IntPoint:
                    re.Write(((IntPointData)value).X);
                    re.Write(((IntPointData)value).Y);
                    break;
                case TagType.UIntPoint:
                    re.Write((int)((UIntPointData)value).X);
                    re.Write((int)((UIntPointData)value).Y);
                    break;
                case TagType.IntPoint3:
                    re.Write(((IntPoint3Data)value).X);
                    re.Write(((IntPoint3Data)value).Y);
                    re.Write(((IntPoint3Data)value).Z);
                    break;
                case TagType.UIntPoint3:
                    re.Write((int)((UIntPoint3Data)value).X);
                    re.Write((int)((UIntPoint3Data)value).Y);
                    re.Write((int)((UIntPoint3Data)value).Z);
                    break;
                case TagType.LongPoint:
                    re.Write(((LongPointData)value).X);
                    re.Write(((LongPointData)value).Y);
                    break;
                case TagType.ULongPoint:
                    re.Write((long)((ULongPointData)value).X);
                    re.Write((long)((ULongPointData)value).Y);
                    break;
                case TagType.LongPoint3:
                    re.Write(((LongPoint3Data)value).X);
                    re.Write(((LongPoint3Data)value).Y);
                    re.Write(((LongPoint3Data)value).Z);
                    break;
                case TagType.ULongPoint3:
                    re.Write((long)((ULongPoint3Data)value).X);
                    re.Write((long)((ULongPoint3Data)value).Y);
                    re.Write((long)((ULongPoint3Data)value).Z);
                    break;
            }
            re.WriteByte(quality);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="re"></param>
        private void SetTagValueToBuffer2(TagType type, object value, byte quality, ByteBuffer re)
        {
            switch (type)
            {
                case TagType.Bool:
                    re.Write(Convert.ToByte(value));
                    break;
                case TagType.Byte:
                    re.Write(Convert.ToByte(value));
                    break;
                case TagType.Short:
                    re.Write(Convert.ToInt16(value));
                    break;
                case TagType.UShort:
                    re.Write(Convert.ToUInt16(value));
                    break;
                case TagType.Int:
                    re.Write(Convert.ToInt32(value));
                    break;
                case TagType.UInt:
                    re.Write(Convert.ToUInt32(value));
                    break;
                case TagType.Long:
                case TagType.ULong:
                    re.Write(Convert.ToInt64(value));
                    break;
                case TagType.Float:
                    re.Write(Convert.ToSingle(value));
                    break;
                case TagType.Double:
                    re.Write(Convert.ToDouble(value));
                    break;
                case TagType.String:
                    string sval = value.ToString();
                    //re.WriteInt(sval.Length);
                    re.Write(sval, Encoding.Unicode);
                    break;
                case TagType.DateTime:
                    re.Write(((DateTime)value).ToBinary());
                    break;
                case TagType.IntPoint:
                    re.Write(((IntPointData)value).X);
                    re.Write(((IntPointData)value).Y);
                    break;
                case TagType.UIntPoint:
                    re.Write((int)((UIntPointData)value).X);
                    re.Write((int)((UIntPointData)value).Y);
                    break;
                case TagType.IntPoint3:
                    re.Write(((IntPoint3Data)value).X);
                    re.Write(((IntPoint3Data)value).Y);
                    re.Write(((IntPoint3Data)value).Z);
                    break;
                case TagType.UIntPoint3:
                    re.Write((int)((UIntPoint3Data)value).X);
                    re.Write((int)((UIntPoint3Data)value).Y);
                    re.Write((int)((UIntPoint3Data)value).Z);
                    break;
                case TagType.LongPoint:
                    re.Write(((LongPointData)value).X);
                    re.Write(((LongPointData)value).Y);
                    break;
                case TagType.ULongPoint:
                    re.Write((long)((ULongPointData)value).X);
                    re.Write((long)((ULongPointData)value).Y);
                    break;
                case TagType.LongPoint3:
                    re.Write(((LongPoint3Data)value).X);
                    re.Write(((LongPoint3Data)value).Y);
                    re.Write(((LongPoint3Data)value).Z);
                    break;
                case TagType.ULongPoint3:
                    re.Write((long)((ULongPoint3Data)value).X);
                    re.Write((long)((ULongPoint3Data)value).Y);
                    re.Write((long)((ULongPoint3Data)value).Z);
                    break;
            }
            re.WriteByte(quality);
        }


        ///// <summary>
        ///// 设置一组变量的实时值
        ///// </summary>
        ///// <param name="data"></param>
        ///// <param name="timeout"></param>
        ///// <returns></returns>
        //public bool SetTagValue(RealDataBuffer data, int timeout = 5000)
        //{
        //    CheckLogin();
        //    if (data.Position <= 0) return false;

        //    var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + (int)data.Position);
        //    mb.WriteByte(ApiFunConst.SetTagValueFun);
        //    mb.WriteLong(this.mLoginId);
        //    mb.WriteInt(data.ValueCount);

        //    System.Runtime.InteropServices.Marshal.Copy(data.Buffers, mb.Array, mb.ArrayOffset + mb.WriterIndex, (int)data.Position);
        //    mb.SetWriterIndex((int)(mb.WriterIndex + data.Position));
            
        //    realRequreEvent.Reset();
        //    Send(mb);

        //    try
        //    {
        //        if (realRequreEvent.WaitOne(timeout))
        //        {
        //            return mRealRequreData != null && mRealRequreData.ReadableBytes > 1;
        //        }
        //    }
        //    catch
        //    {

        //    }

        //    return false;
        //}

        /// <summary>
        /// 设置一组变量的实时和历史值
        /// 对于历史记录类型为驱动时，起作用
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValue(RealDataBuffer data, int timeout = 5000)
        {
            CheckLogin();

            if (data.Position <= 0 || mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + (int)data.Position);
            mb.Write(ApiFunConst.SetTagRealAndHisValueFun);
            mb.Write(this.mLoginId);
            mb.Write(data.ValueCount);

            mb.Write(data.Buffers, (int)data.Position);

            //System.Runtime.InteropServices.Marshal.Copy(data.Buffers, mb.Array, mb.ArrayOffset + mb.WriterIndex, (int)data.Position);
            //mb.SetWriterIndex((int)(mb.WriterIndex + data.Position));

            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 1;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();
                }
                
            }
            

            return false;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="data"></param>
        ///// <param name="timeout"></param>
        ///// <returns></returns>
        //public bool SetTagValueAsync(RealDataBuffer data, int timeout = 5000)
        //{
        //    CheckLogin();
        //    if (data.Position <= 0) return false;

        //    var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position);
        //    mb.WriteByte(ApiFunConst.SetTagValueFun);
        //    mb.WriteLong(this.mLoginId);
        //    mb.WriteInt(data.ValueCount);
        //    System.Runtime.InteropServices.Marshal.Copy(data.Buffers, mb.Array, mb.ArrayOffset + mb.WriterIndex, (int)data.Position);
        //    mb.SetWriterIndex((int)(mb.WriterIndex + data.Position));
        //    Send(mb);
        //    return true;
        //}

        /// <summary>
        /// 设置变量的实时、历史值
        /// 历史值只有在变量的记录类型为：驱动时起作用
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValueAsync(RealDataBuffer data, int timeout = 5000)
        {
            CheckLogin();

            if (data.Position <= 0 || mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position);
            mb.Write(ApiFunConst.SetTagRealAndHisValueFun);
            mb.Write(this.mLoginId);
            mb.Write(data.ValueCount);

            mb.Write(data.Buffers, (int)data.Position);
            //System.Runtime.InteropServices.Marshal.Copy(data.Buffers, mb.Array, mb.ArrayOffset + mb.WriterIndex, (int)data.Position);
            //mb.SetWriterIndex((int)(mb.WriterIndex + data.Position));
            SendData(mb);
            return true;
        }

        /// <summary>
        /// 设置一组变量的实时值
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValueAndQuality(RealDataBuffer data, int timeout = 5000)
        {
            CheckLogin();

            if (mIsRealDataBusy) return false;

            //Stopwatch sw = new Stopwatch();
            //sw.Start();

            var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position);
            mb.Write(ApiFunConst.SetTagValueAndQualityFun);
            mb.Write(this.mLoginId);
            mb.Write(data.ValueCount);

            mb.Write(data.Buffers, (int)data.Position);
            //System.Runtime.InteropServices.Marshal.Copy(data.Buffers, mb.Array, mb.ArrayOffset + mb.WriterIndex, (int)data.Position);
            //mb.SetWriterIndex((int)(mb.WriterIndex + data.Position));

            realRequreEvent.Reset();

            //long ltmp = sw.ElapsedMilliseconds;

            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 1;
                }
                finally
                {
                    mRealRequreData?.UnlockAndReturn();
                }
            }

            //try
            //{
               
            //}
            //catch
            //{

            //}
            //finally
            //{
               
            //    sw.Stop();
            //    Debug.Print("SetTagValueAndQuality:" + sw.ElapsedMilliseconds + " , " + ltmp);
            //}

            return false;
        }

        /// <summary>
        /// 设置一组变量的实时值,
        /// 立即返回
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValueAndQualityAsync(RealDataBuffer data, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position);
            mb.Write(ApiFunConst.SetTagValueAndQualityFun);
            mb.Write(this.mLoginId);
            mb.Write(data.ValueCount);

            mb.Write(data.Buffers, (int)data.Position);
            //System.Runtime.InteropServices.Marshal.Copy(data.Buffers, mb.Array, mb.ArrayOffset + mb.WriterIndex, (int)data.Position);
            //mb.SetWriterIndex((int)(mb.WriterIndex + data.Position));

            realRequreEvent.Reset();
            SendData(mb);

            return true;
        }


        ///// <summary>
        ///// 设置一组变量的实时值
        ///// </summary>
        ///// <param name="ids">ID，值类型，值</param>
        ///// <param name="timeout">超时</param>
        ///// <returns></returns>
        //public bool SetTagValue(List<RealTagValue> ids, int timeout = 5000)
        //{
        //    CheckLogin();
        //    var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + ids.Count * 32);
        //    mb.WriteByte(ApiFunConst.SetTagValueFun);
        //    mb.WriteLong(this.mLoginId);
        //    mb.WriteInt(ids.Count);
        //    foreach (var vv in ids)
        //    {
        //        mb.WriteInt(vv.Id);
        //        SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, mb);
        //    }
        //    realRequreEvent.Reset();
        //    Send(mb);

        //    try
        //    {
        //        if (realRequreEvent.WaitOne(timeout))
        //        {
        //            return mRealRequreData != null && mRealRequreData.ReadableBytes>1;
        //        }
        //    }
        //    catch
        //    {

        //    }

        //    return false;
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValue(List<RealTagValue> ids, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + ids.Count * 32);
            mb.Write(ApiFunConst.SetTagRealAndHisValueFun);
            mb.Write(this.mLoginId);
            mb.Write(ids.Count);
            foreach (var vv in ids)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value,vv.Quality, mb);
            }
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 1;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();
                }
               
            }

            
            return false;
        }


        /// <summary>
        /// 设置变量的实时、历史值
        /// 历史值只有在变量的记录类型为：驱动时起作用
        /// </summary>
        /// <param name="values">ID，值类型，值，质量</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValueAndQuality(List<RealTagValue> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 32);
            mb.Write(ApiFunConst.SetTagValueAndQualityFun);
            mb.Write(this.mLoginId);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value,vv.Quality, mb);
            }
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 1;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();
                    
                }
               
            }

            
            return false;
        }

        /// <summary>
        /// 设置变量的实时、历史值.
        /// 历史值只有在变量的记录类型为：驱动时起作用
        /// 立即返回
        /// </summary>
        /// <param name="values">ID，值类型，值，质量</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValueAndQualityAsync(List<RealTagValue> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;
            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 32);
            mb.Write(ApiFunConst.SetTagValueAndQualityFun);
            mb.Write(this.mLoginId);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, vv.Quality, mb);
            }
            realRequreEvent.Reset();
            SendData(mb);
            return true;
        }

        /// <summary>
        /// 订购指定变量的值改变通知信息
        /// </summary>
        /// <param name="ids">Id集合</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool AppendRegistorDataChangedCallBack(IEnumerable<int> ids,int timeout=5000)
        {
            CheckLogin();
           
            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + ids.Count() * 4);
            mb.Write(ApiFunConst.RegistorTag);
            mb.Write(this.mLoginId);
            mb.Write(ids.Count());
            foreach (var vv in ids)
            {
                mb.Write(vv);
            }
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 1;
                }
                finally
                {
                    mRealRequreData?.UnlockAndReturn();
                }
            }
            return false;
        }

        /// <summary>
        /// 取消订购指定变量的值改变通知信息
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool UnRegistorDataChangedCallBack(IEnumerable<int> ids, int timeout = 5000)
        {
            CheckLogin();
         
            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + ids.Count() * 4);
            mb.Write(ApiFunConst.UnRegistorTag);
            mb.Write(this.mLoginId);
            mb.Write(ids.Count());
            foreach (var vv in ids)
            {
                mb.Write(vv);
            }
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 1;
                }
                finally
                {
                    mRealRequreData?.UnlockAndReturn();
                }
            }
            return false;
        }

        /// <summary>
        /// 取消所有值改变订购信息
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool ResetRegistorDataChangedCallBack(int timeout = 5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.RealValueFun, 13);
            mb.Write(ApiFunConst.ClearRegistorTag);
            mb.Write(this.mLoginId);
            mb.Write(1);
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 1;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();
                }
                
            }

           
            return false;
        }


        /// <summary>
        /// 设置变量的一组历史值
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="type">值类型</param>
        /// <param name="values">值集合</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public bool SetTagHisValue(int id, TagType type, IEnumerable<TagValue> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsHisDataBusy) return false;
            var mb = GetBuffer(ApiFunConst.HisValueFun, 18 + values.Count() * 33);
            mb.Write(ApiFunConst.SetTagHisValue);
            mb.Write(this.mLoginId);
            mb.Write(id);
            mb.Write(values.Count());
            mb.Write((byte)type);
            

            foreach (var vv in values)
            {
                mb.Write(vv.Time.ToBinary());
                SetTagValueToBuffer2(type, vv.Value,vv.Quality, mb);
            }
            hisRequreEvent.Reset();
            SendData(mb);

            if (this.hisRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 1;
                }
                catch
                {
                    mHisRequreData?.UnlockAndReturn();
                }
              
            }
            

            return false;
        }

        /// <summary>
        /// 设置变量的一组历史值
        /// </summary>
        /// <param name="id"></param>
        /// <param name="type"></param>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagHisValue(int id, TagType type, HisDataBuffer data, int timeout = 5000)
        {
            CheckLogin();
            if (mIsHisDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.HisValueFun, 18 + (int)data.Position);
            mb.Write(ApiFunConst.SetTagHisValue);
            mb.Write(this.mLoginId);
            mb.Write(id);
            mb.Write(data.ValueCount);
            mb.Write((byte)type);

            //System.Runtime.InteropServices.Marshal.Copy(data.Buffers, mb.Array, mb.ArrayOffset + mb.WriterIndex, (int)data.Position);
            //mb.SetWriterIndex((int)(mb.WriterIndex + data.Position));

            mb.Write(data.Buffers, (int)data.Position);

            hisRequreEvent.Reset();
            SendData(mb);

            if (this.hisRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 1;
                }
                catch
                {
                    mHisRequreData?.UnlockAndReturn();
                }
               
            }

            

            return false;
        }

        /// <summary>
        /// 设置一组变量的历史值
        /// </summary>
        /// <param name="idvalues">ID，值集合</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetMutiTagHisValue(Dictionary<int,TagValueAndType> idvalues,int timeout=5000)
        {
            if (idvalues == null && idvalues.Count == 0) return false;
            if (mIsHisDataBusy) return false;

            CheckLogin();
            var mb = GetBuffer(ApiFunConst.HisValueFun, 13 + idvalues.Count * 38);
            mb.Write(ApiFunConst.SetTagHisValue2);
            mb.Write(this.mLoginId);
            mb.Write(idvalues.Count);

            foreach (var vv in idvalues)
            {
                mb.Write(vv.Key);
                mb.Write(vv.Value.Time.ToBinary());
                mb.Write((byte)vv.Value.ValueType);
                SetTagValueToBuffer2(vv.Value.ValueType, vv.Value.Value, vv.Value.Quality, mb);
            }

            hisRequreEvent.Reset();
            SendData(mb);

            if (this.hisRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 1;
                }
                catch
                {
                    mHisRequreData?.UnlockAndReturn();
                }
              
            }

            
            return false;
        }

        /// <summary>
        /// 设置一组变量的历史值
        /// </summary>
        /// <param name="idvalues">ID，值集合</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetMutiTagHisValue(HisDataBuffer data, int timeout = 5000)
        {
            if (data == null && data.ValueCount == 0) return false;
            if (mIsHisDataBusy) return false;

            CheckLogin();
            var mb = GetBuffer(ApiFunConst.HisValueFun, 13 + (int)data.Position);
            mb.Write(ApiFunConst.SetTagHisValue2);
            mb.Write(this.mLoginId);
            mb.Write(data.ValueCount);

            //System.Runtime.InteropServices.Marshal.Copy(data.Buffers, mb.Array, mb.ArrayOffset + mb.WriterIndex, (int)data.Position);
            //mb.SetWriterIndex((int)(mb.WriterIndex + data.Position));

            mb.Write(data.Buffers, (int)data.Position);

            hisRequreEvent.Reset();
            SendData(mb);

            if (this.hisRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 1;
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                }
            }
            return false;
        }

        /// <summary>
        /// 设置变量的历史值
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="values">值</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public bool SetTagHisValue(int id, TagValueAndType values,  int timeout = 5000)
        {
            CheckLogin();

            if (mIsHisDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.HisValueFun, 26 + 40);
            mb.Write(ApiFunConst.SetTagHisValue2);
            mb.Write(this.mLoginId);
            mb.Write(1);

            mb.Write(id);
            mb.Write(values.Time.ToBinary());
            mb.Write((byte)values.ValueType);
            SetTagValueToBuffer2(values.ValueType, values.Value, values.Quality, mb);

            hisRequreEvent.Reset();
            SendData(mb);

            if (this.hisRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 1;
                }
                catch
                {
                    mHisRequreData?.UnlockAndReturn();
                }
               
            }


            return false;
        }

        /// <summary>
        /// 获取变量的ID
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="timeout"></param>
        /// <returns>ID 集合</returns>
        public List<int> QueryTagId(IEnumerable<string> tags,int timeout = 5000)
        {
            lock (mTagInfoLockObj)
            {
                List<int> re = new List<int>();
                CheckLogin();
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 13 + tags.Count() * 256);
                mb.Write(ApiFunConst.GetTagIdByNameFun);
                mb.Write(this.mLoginId);
                mb.Write(tags.Count());
                foreach (var vv in tags)
                {
                    mb.Write(vv);
                }
                infoRequreEvent.Reset();
                SendData(mb);

                try
                {
                    if (infoRequreEvent.WaitOne(timeout) && mInfoRequreData.Count > 0)
                    {
                        var vdata = mInfoRequreData.Dequeue();
                        if (vdata != null)
                        {
                            var count = vdata.ReadInt();
                            for (int i = 0; i < count; i++)
                            {
                                re.Add(vdata.ReadInt());
                            }
                            vdata.UnlockAndReturn();
                        }
                    }
                }
                catch
                {

                }
                return re;
            }
        }

        /// <summary>
        /// 该驱动对应的获取所有变量的ID，名称，类型
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns>ID、名称、类型集合</returns>
        public Dictionary<int, Tuple<string, byte>> QueryAllTagIdAndNames(int timeout = 5000)
        {
            lock (mTagInfoLockObj)
            {
                Dictionary<int, Tuple<string, byte>> re = new Dictionary<int, Tuple<string, byte>>();
                CheckLogin();
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 9);
                mb.Write(ApiFunConst.QueryAllTagNameAndIds);
                mb.Write(this.mLoginId);
                DateTime dt = DateTime.Now;
                infoRequreEvent.Reset();
                lock (mInfoRequreData)
                {
                    while(mInfoRequreData.Count>0)
                    {
                        mInfoRequreData.Dequeue().UnlockAndReturn();
                    }
                    mInfoRequreData.Clear();
                }
                SendData(mb);
                try
                {
                    if (infoRequreEvent.WaitOne(timeout) && mInfoRequreData.Count > 0)
                    {
                        while (true)
                        {
                            if (mInfoRequreData.Count > 0)
                            {
                                var vdata = mInfoRequreData.Peek();
                                var cmd = vdata.ReadByte();
                                if (cmd == ApiFunConst.QueryAllTagNameAndIds)
                                {
                                    int total = vdata.ReadShort();
                                    int icount = vdata.ReadShort();
                                    int tcount = vdata.ReadInt();
                                    string name = "";
                                    for (int i = 0; i < tcount; i++)
                                    {
                                        var id = vdata.ReadInt();
                                        if(id==0)
                                        {
                                            Debug.Print("数据错误:" + id);
                                        }
                                         name = vdata.ReadString();
                                        var type = vdata.ReadByte();
                                        if (!re.ContainsKey(id))
                                        {
                                            re.Add(id, new Tuple<string, byte>(name, type));
                                        }
                                        //else
                                        //{
                                        //    Debug.Print("重复ID"+id);
                                        //}
                                    }

                                    lock (mInfoRequreData)
                                    {
                                        var vd = mInfoRequreData.Dequeue();
                                        vd.UnlockAndReturn();
                                    }
                                    if (icount >= (total - 1)) break;
                                }
                                else
                                {
                                    break;
                                }
                            }
                            else
                            {
                                if ((DateTime.Now - dt).TotalMilliseconds > timeout)
                                {
                                    break;
                                }
                                else
                                {
                                    Thread.Sleep(100);
                                }
                            }
                        }

                    }
                }
                catch
                {

                }
                return re;
            }
        }

        /// <summary>
        /// 获取所有驱动更新的历史记录的变量的ID
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns>变量ID集合</returns>
        public List<int> GetDriverRecordTypeTagIds(int timeout = 5000)
        {
            lock (mTagInfoLockObj)
            {
                List<int> re = new List<int>();
                CheckLogin();
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 9);
                mb.Write(ApiFunConst.GetDriverRecordTypeTagIds);
                mb.Write(this.mLoginId);
                DateTime dt = DateTime.Now;
                infoRequreEvent.Reset();
                lock (mInfoRequreData)
                {
                    while (mInfoRequreData.Count > 0)
                    {
                        mInfoRequreData.Dequeue().UnlockAndReturn();
                    }
                    mInfoRequreData.Clear();
                }
                SendData(mb);
                try
                {
                    if (infoRequreEvent.WaitOne(timeout) && mInfoRequreData.Count > 0)
                    {
                        while (true)
                        {
                            if (mInfoRequreData.Count > 0)
                            {
                                var vdata = mInfoRequreData.Peek();
                                var cmd = vdata.ReadByte();
                                if (cmd == ApiFunConst.GetDriverRecordTypeTagIds)
                                {
                                    int total = vdata.ReadShort();
                                    int icount = vdata.ReadShort();
                                    int tcount = vdata.ReadInt();
                                    for (int i = 0; i < tcount; i++)
                                    {
                                        var id = vdata.ReadInt();
                                        re.Add(id);
                                    }

                                    lock (mInfoRequreData)
                                    {
                                        var vd = mInfoRequreData.Dequeue();
                                        vd.UnlockAndReturn();
                                    }
                                    if (icount >= (total - 1)) break;
                                }
                                else
                                {
                                    break;
                                }
                            }
                            else
                            {
                                if ((DateTime.Now - dt).TotalSeconds > timeout)
                                {
                                    break;
                                }
                                else
                                {
                                    Thread.Sleep(100);
                                }
                            }
                        }

                    }
                }
                catch
                {

                }
                return re;
            }
        }

        /// <summary>
        /// 检查变量的记录类型是否为驱动自主更新类型
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public List<bool> CheckRecordTypeByTagId(IEnumerable<int> ids, int timeout = 5000)
        {
            lock (mTagInfoLockObj)
            {
                List<bool> re = new List<bool>();
                CheckLogin();
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 13 + ids.Count() * 4);
                mb.Write(ApiFunConst.GetDriverRecordTypeTagIds2);
                mb.Write(this.mLoginId);
                mb.Write(ids.Count());
                foreach (var vv in ids)
                {
                    mb.Write(vv);
                }
                DateTime dt = DateTime.Now;
                infoRequreEvent.Reset();
                lock (mInfoRequreData)
                {
                    while (mInfoRequreData.Count > 0)
                    {
                        mInfoRequreData.Dequeue().UnlockAndReturn();
                    }
                    mInfoRequreData.Clear();
                }
                SendData(mb);
                try
                {
                    if (infoRequreEvent.WaitOne(timeout) && mInfoRequreData.Count > 0)
                    {
                        var vdata = mInfoRequreData.Peek();
                        var cmd = vdata.ReadByte();
                        if (cmd == ApiFunConst.GetDriverRecordTypeTagIds2)
                        {
                            int tcount = vdata.ReadInt();
                            for (int i = 0; i < tcount; i++)
                            {
                                re.Add(vdata.ReadByte() > 0);
                            }

                            lock (mInfoRequreData)
                            {
                                 mInfoRequreData.Dequeue().UnlockAndReturn();
                               
                            }
                        }

                    }
                }
                catch
                {

                }
                return re;
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
