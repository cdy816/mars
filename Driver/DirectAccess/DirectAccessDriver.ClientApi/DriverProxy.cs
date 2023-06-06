//==============================================================
//  Copyright (C) 2022  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/1/2 11:00:57.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Cdy.Tag;
using System.Linq;
using Cheetah;
using System.Drawing;
using System.Security.Cryptography;

namespace DirectAccessDriver.ClientApi
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="isrealchanged"></param>
        /// <param name="ishischanged"></param>
        public delegate void DatabaseChangedDelegate(bool isrealchanged, bool ishischanged);

        private string mUser, mPass;

        private object mTagInfoLockObj = new object();

        private bool mIsRealDataBusy = false;
        private bool mIsHisDataBusy = false;

        private long mCallId = 0;

        private Dictionary<long, CallContext> mCallBackDatas = new Dictionary<long, CallContext>();

        public Dictionary<int, Action<ByteBuffer>> mHisDataCallBack = new Dictionary<int, Action<ByteBuffer>>();

        public Dictionary<int, Action<ByteBuffer>> mRealDataCallBack = new Dictionary<int, Action<ByteBuffer>>();

        private int mHisRequreCount = 0;

        private object mHisDataLock=new object();

        private object mRealDataLock = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...


        /// <summary>
        /// 
        /// </summary>
        public long LoginId { get { return mLoginId; } }

        /// <summary>
        /// 是否登录
        /// </summary>
        public bool IsLogin { get { return mLoginId > 0; } }

        /// <summary>
        /// 
        /// </summary>
        public string UserName { get { return mUser; } set { mUser = value; } }

        /// <summary>
        /// 
        /// </summary>
        public string Password { get { return mPass; } set { mPass = value; } }

        /// <summary>
        /// 变量的值改变回调
        /// </summary>
        public ProcessDataPushDelegate ValueChanged { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DatabaseChangedDelegate DatabaseChanged { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public long GetCallId()
        {
            lock (mCallBackDatas)
                return ++mCallId;
        }

        /// <summary>
        /// 退出登录
        /// </summary>
        public void Logout()
        {
            mLoginId = -1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="isConnected"></param>
        public override void OnConnected(bool isConnected)
        {
            base.OnConnected(isConnected);
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
                case ApiFunConst.DatabaseChangedNotify:
                    var changeddata = datas.ReadByte();
                    DatabaseChanged?.Invoke((changeddata & 0x01)>0, ((byte)(changeddata >> 1) & 0x01)>0);
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
                case ApiFunConst.AysncValueBack:
                    var vid = datas.ReadLong();
                    lock (mCallBackDatas)
                    {
                        if (mCallBackDatas.ContainsKey(vid))
                        {
                            mCallBackDatas[vid].Data = datas;
                            mCallBackDatas[vid].CallBackAction(mCallBackDatas[vid]);
                        }
                        else
                        {
                            datas?.UnlockAndReturn();
                        }
                    }
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
                case ApiFunConst.ReadTagHisValueReturn:
                    var cid = datas.ReadInt();
                    ProcessHisDataCallBack(cid, datas);
                    break;
                case ApiFunConst.ReadTagRealValue:
                    var rid = datas.ReadInt();
                    ProcessRealDataCallBack(rid, datas);
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
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="mHisRequreData"></param>
        private void ProcessHisDataCallBack(int id, ByteBuffer mHisRequreData)
        {
            lock (mHisDataCallBack)
            {
                if (mHisDataCallBack.ContainsKey(id))
                {
                    mHisDataCallBack[id].Invoke(mHisRequreData);
                    mHisDataCallBack.Remove(id);
                }
                else
                {
                    mHisRequreData.UnlockAndReturn();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="mHisRequreData"></param>
        private void ProcessRealDataCallBack(int id, ByteBuffer mHisRequreData)
        {
            Stopwatch sw = Stopwatch.StartNew();
            sw.Start();
            lock (mRealDataCallBack)
            {
                if (mRealDataCallBack.ContainsKey(id))
                {
                    mRealDataCallBack[id].Invoke(mHisRequreData);
                    mRealDataCallBack.Remove(id);
                }
                else
                {
                    mHisRequreData.UnlockAndReturn();
                }
            }
            sw.Stop();

            if (sw.ElapsedMilliseconds > 100)
            {
                LoggerService.Service.Warn("DriverProxy", $"ProcessRealDataCallBack 处理耗时： {sw.ElapsedMilliseconds}");
            }
        }

        /// <summary>
        /// 心跳
        /// </summary>
        public void Heart()
        {
            var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 9);
            mb.Write(ApiFunConst.Hart);
            mb.Write(this.mLoginId);
            infoRequreEvent.Reset();
            SendData(mb);
        }

        /// <summary>
        /// 使用当前账户进行登录
        /// </summary>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public bool Login(int timeout=30000)
        {
            return Login(UserName,Password,timeout);
        }

        /// <summary>
        /// 登录
        /// </summary>
        /// <param name="username">用户名</param>
        /// <param name="password">密码</param>
        /// <param name="timeount">超时</param>
        /// <returns>是否成功</returns>
        public bool Login(string username, string password, int timeount = 30000)
        {
            lock (mTagInfoLockObj)
            {
                mInfoRequreData.Clear();
                mUser = username;
                mPass = password;
                int size = username.Length + password.Length + 9;
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, size);
                mb.Write(ApiFunConst.LoginFun);
                mb.Write(username);
                mb.Write(password);
                infoRequreEvent.Reset();
                SendData(mb);
                if (infoRequreEvent.WaitOne(timeount))
                {
                    while (mInfoRequreData.Count > 0)
                    {
                        var vdata = mInfoRequreData.Dequeue();
                        try
                        {
                            if (vdata.ReadableCount > 4)
                            {
                                var cid = vdata.ReadByte();
                                if (cid == ApiFunConst.LoginFun)
                                {
                                    mLoginId = vdata.ReadLong();
                                    if (mLoginId < 0)
                                    {
                                        Console.WriteLine("Username or password is invailed!");
                                    }
                                    //else
                                    //{
                                    //    Console.WriteLine("Login sucessfull:" + mLoginId);
                                    //}

                                    return IsLogin;
                                }
                              
                            }
                        }
                        finally
                        {
                            vdata?.UnlockAndReturn();
                        }
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
        }

        /// <summary>
        /// 登录到数据库，并返回Token。用于对外提供账户登录服务，而非自身登录。
        /// </summary>
        /// <param name="username">用户名</param>
        /// <param name="password">密码</param>
        /// <param name="timeount">超时</param>
        /// <returns></returns>
        public string Login2(string username, string password, int timeount = 5000)
        {
            lock (mTagInfoLockObj)
            {
                mInfoRequreData.Clear();
                mUser = username;
                mPass = password;
                int size = username.Length + password.Length + 9+8;
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, size);
                mb.Write(ApiFunConst.Login2Fun);
                mb.Write(this.mLoginId);
                mb.Write(username);
                mb.Write(password);
                infoRequreEvent.Reset();
                SendData(mb);
                if (infoRequreEvent.WaitOne(timeount) && mInfoRequreData.Count > 0)
                {
                    while (mInfoRequreData.Count > 0)
                    {
                        var vdata = mInfoRequreData.Dequeue();
                        try
                        {
                            if (vdata.ReadableCount > 4)
                            {
                                var cid = vdata.ReadByte();
                                if (cid == ApiFunConst.Login2Fun)
                                {
                                    return vdata.ReadString();
                                }
                                else
                                {
                                    return string.Empty;
                                }

                            }
                        }
                        finally
                        {
                            vdata?.UnlockAndReturn();
                        }
                    }
                }
                return string.Empty;
            }
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
        private void SetTagValueToBuffer(TagType type, object value,DateTime time, byte quality, ByteBuffer re)
        {
            re.Write((byte)type);
            re.Write(time.Ticks);
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="re"></param>
        private void SetTagValueToBuffer3(TagType type, object value, byte quality, ByteBuffer re)
        {
            re.Write((byte)type);
            SetTagValueToBuffer2(type,value,quality,re);
        }

        #region RealValue

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
        /// 设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValue(RealDataBuffer data, int timeout = 5000)
        {
            CheckLogin();

            if (data.Position <= 0 || mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + (int)data.Position + 4);
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
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();
                }
                
            }
            

            return false;
        }

        
        private CallContext GetContext(long vid,Action<CallContext> callback)
        {
            return new CallContext() { Id=vid,CallBackAction=callback};
        }

        /// <summary>
        /// 采用异步通讯的方式，设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// 支持多线程调用
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public void SetTagRealAndHisValueAsync(RealDataBuffer data,Action<bool> callback, int timeout = 5000)
        {
            CheckLogin();

            if (data.Position <= 0 || mIsRealDataBusy) callback?.Invoke(false);

            var vid = this.GetCallId();
            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + (int)data.Position + 12);
            mb.Write(ApiFunConst.SetTagRealAndHisValueFunAsync);
            mb.Write(this.mLoginId);
            mb.Write(vid);
            mb.Write(data.ValueCount);
            mb.Write(data.Buffers, (int)data.Position);

            CallContext ctx = GetContext(vid, data => {
                if ((DateTime.Now - data.StartTime).TotalMilliseconds < data.Timeout)
                {
                    callback?.Invoke(data.Data?.ReadableCount > 0);
                }
                else
                {
                    callback?.Invoke(false);
                }

                lock (mCallBackDatas)
                {
                    if (mCallBackDatas.ContainsKey(data.Id))
                        mCallBackDatas.Remove(data.Id);
                }

                data.Data?.UnlockAndReturn();
                data.Data = null;
                data.CallBackAction = null;

            });
            ctx.StartTime = DateTime.Now;
            ctx.Timeout = timeout;

            lock (mCallBackDatas)
            {
                mCallBackDatas.Add(vid, ctx);
            }
            
            SendData(mb);

            
        }


        /// <summary>
        /// 设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// 不需要等待服务器返回响应，不需要预先登录，每次请求均带着用户名、密码,用于数据单向传输
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValue2(RealDataBuffer data)
        {
            if (data.Position <= 0 || mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + (int)data.Position+64);
            mb.Write(ApiFunConst.SetTagRealAndHisValueWithUserFun);
            mb.Write(this.mUser);
            mb.Write(this.mPass);
            mb.Write(data.ValueCount);
            mb.Write(data.Buffers, (int)data.Position);
            SendData(mb);
            return IsConnected;
        }

        /// <summary>
        /// 通过变量的名称，设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValue(List<RealTagValue3> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 32);
            mb.Write(ApiFunConst.SetTagRealAndHisValueWithTagNameFun);
            mb.Write(this.mLoginId);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.TagName);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, vv.Quality, mb);
            }
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();

                }

            }


            return false;
        }

        /// <summary>
        /// 通过变量的ID，设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValue(List<RealTagValue> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 32);
            mb.Write(ApiFunConst.SetTagRealAndHisValueFun);
            mb.Write(this.mLoginId);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, vv.Quality, mb);
            }
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();

                }

            }


            return false;
        }


        /// <summary>
        /// 通过变量的ID，异步设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public void SetTagRealAndHisValueAsync(List<RealTagValue> values,Action<bool> callback, int timeout = 5000)
        {
            CheckLogin();

            if (values==null || values.Count <= 0 || mIsRealDataBusy) callback?.Invoke(false);

            var vid = this.GetCallId();

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 32);
            mb.Write(ApiFunConst.SetTagRealAndHisValueFunAsync);
            mb.Write(this.mLoginId);
            mb.Write(vid);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, vv.Quality, mb);
            }
            RegistorCallBackContext(vid,timeout,callback);
            SendData(mb);
        }


        /// <summary>
        /// 通过变量ID，设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValueWithTimer(List<RealTagValueWithTimer> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 40);
            mb.Write(ApiFunConst.SetTagRealAndHisValueWithTimeFun);
            mb.Write(this.mLoginId);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value,vv.Time, vv.Quality, mb);
            }
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();

                }

            }


            return false;
        }


        /// <summary>
        /// 通过变量名称，设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValueWithTimer(List<RealTagValueWithTimer2> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 40);
            mb.Write(ApiFunConst.SetTagRealAndHisValueWithTimeFun);
            mb.Write(this.mLoginId);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.TagName);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, vv.Time, vv.Quality, mb);
            }
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();

                }

            }


            return false;
        }

        private CallContext RegistorCallBackContext(long vid,int timeout, Action<bool> callback)
        {
            var ctx = GetContext(vid, data => {

                if ((DateTime.Now - data.StartTime).TotalMilliseconds < data.Timeout)
                {
                    callback?.Invoke(data.Data?.ReadableCount > 0);
                }
                else
                {
                    callback?.Invoke(false);
                }

                lock (mCallBackDatas)
                {
                    if (mCallBackDatas.ContainsKey(data.Id))
                        mCallBackDatas.Remove(data.Id);
                }

                data.Data?.UnlockAndReturn();
                data.Data = null;
                data.CallBackAction = null;
            });
            ctx.StartTime = DateTime.Now;
            ctx.Timeout = timeout;
            lock (mCallBackDatas)
            {
                mCallBackDatas.Add(vid, ctx);
            }
            return ctx;
        }

        /// <summary>
        /// 通过变量的ID，设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// </summary>
        /// <param name="data">实时数据块</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValueWithTimer(RealDataBuffer data, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + (int)data.Position+1);
            mb.Write(ApiFunConst.SetTagRealAndHisValueWithTimeFun);
            mb.Write(this.mLoginId);
            mb.Write(data.ValueCount);
            mb.Write(data.Buffers, (int)data.Position);
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();

                }

            }


            return false;
        }


        ///// <summary>
        ///// 采用异步通讯的方式，设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        ///// 对于历史,记录类型为驱动时起作用
        ///// 支持多线程调用
        ///// </summary>
        ///// <param name="data">实时数据块</param>
        ///// <param name="timeout"></param>
        ///// <returns></returns>
        //public bool SetTagRealAndHisValueWithTimerAsync(RealDataBuffer data, int timeout = 5000)
        //{
        //    CheckLogin();
        //    if (mIsRealDataBusy) return false;
        //    var vid = GetCallId();

        //    var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + (int)data.Position + 9);
        //    mb.Write(ApiFunConst.SetTagRealAndHisValueWithTimeFunAsync);
        //    mb.Write(this.mLoginId);
        //    mb.Write(vid);
        //    mb.Write(data.ValueCount);
        //    mb.Write(data.Buffers, (int)data.Position);

        //    ManualResetEvent realRequreEvent = new ManualResetEvent(false);
        //    realRequreEvent.Reset();

        //    CallContext ctx = GetContext(vid, data => {
        //        realRequreEvent.Set();

        //    });
        //    lock (mCallBackDatas)
        //    {
        //        mCallBackDatas.Add(vid, ctx);
        //    }
        //    SendData(mb);

        //    try
        //    {
        //        if (!realRequreEvent.WaitOne(timeout))
        //        {
        //            return false;
        //        }
        //        else
        //        {
        //            var re = ctx.Data?.ReadableCount > 0;
        //            return re;
        //        }
        //    }
        //    finally
        //    {
        //        lock (mCallBackDatas)
        //        {
        //            if (mCallBackDatas.ContainsKey(vid))
        //                mCallBackDatas.Remove(vid);
        //        }
        //        ctx.Data?.UnlockAndReturn();
        //        ctx.Data = null;
        //        ctx.CallBackAction = null;
        //        realRequreEvent?.Dispose();
        //    }
        //}


        /// <summary>
        /// 清空超时一直没有返回的数据请求
        /// </summary>
        public void CheckAndRemoveTimeoutData()
        {
            DateTime dnow = DateTime.Now;
            foreach(var vv in mCallBackDatas.Keys.ToArray())
            {
                if(mCallBackDatas.ContainsKey(vv))
                {
                    var vdata = mCallBackDatas[vv];
                    if((dnow - vdata.StartTime).TotalMilliseconds>vdata.Timeout)
                    {
                        lock (mCallBackDatas)
                        {
                            if (mCallBackDatas.ContainsKey(vdata.Id))
                                mCallBackDatas.Remove(vdata.Id);
                        }
                        vdata.Data?.UnlockAndReturn();
                        vdata.Data = null;
                        vdata.CallBackAction = null;
                    }
                }
            }
        }

        /// <summary>
        /// 通过变量的ID，采用异步通讯的方式，设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// 支持多线程调用
        /// </summary>
        /// <param name="data">实时数据块</param>
        /// <param name="callback">回调</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public void SetTagRealAndHisValueWithTimerAsync(RealDataBuffer data,Action<bool> callback, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) callback?.Invoke(false);

            var vid = GetCallId();
            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + (int)data.Position + 9);
            mb.Write(ApiFunConst.SetTagRealAndHisValueWithTimeFunAsync);
            mb.Write(this.mLoginId);
            mb.Write(vid);
            mb.Write(data.ValueCount);
            mb.Write(data.Buffers, (int)data.Position);

            CallContext ctx = GetContext(vid, data => {

                if((DateTime.Now - data.StartTime).TotalMilliseconds<data.Timeout)
                {
                    callback?.Invoke(data.Data?.ReadableCount > 0);
                }
                else
                {
                    callback?.Invoke(false);
                }

                lock (mCallBackDatas)
                {
                    if (mCallBackDatas.ContainsKey(data.Id))
                        mCallBackDatas.Remove(data.Id);
                }

                data.Data?.UnlockAndReturn();
                data.Data = null;
                data.CallBackAction = null;
            });
            lock (mCallBackDatas)
            {
                mCallBackDatas.Add(vid, ctx);
            }
            ctx.StartTime = DateTime.Now;
            ctx.Timeout = timeout;
            SendData(mb);
        }

        /// <summary>
        /// 通过变量的ID，设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public void SetTagRealAndHisValueWithTimerAsync(List<RealTagValueWithTimer> values, Action<bool> callback, int timeout = 5000)
        {
            if (mIsRealDataBusy) callback?.Invoke(false);

            CheckLogin();

            var vid = GetCallId();

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 40);
            mb.Write(ApiFunConst.SetTagRealAndHisValueWithTimeFunAsync);
            mb.Write(this.mLoginId);
            mb.Write(vid);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, vv.Time, vv.Quality, mb);
            }

            RegistorCallBackContext(vid, timeout, callback);

            SendData(mb);

        }

        /// <summary>
        /// 通过变量的名称，设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// 用于数据单向传输
        /// </summary>
        /// <param name="values">ID，值类型，值，质量</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValue2(List<RealTagValue2> values)
        {
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * (32 + 64) + 64);
            mb.Write(ApiFunConst.SetTagRealAndHisValueWithUserFun);
            mb.Write(this.mUser);
            mb.Write(this.mPass);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, vv.Quality, mb);
            }
            SendData(mb);
            return IsConnected;
        }


        /// <summary>
        /// 单向传输， 设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        /// 对于历史,记录类型为驱动时起作用
        /// 用于数据单向传输
        /// </summary>
        /// <param name="values">ID，值类型，值，质量</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagRealAndHisValueWithTimer2(List<RealTagValueWithTimer2> values)
        {
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * (32 + 8 + 64) + 64);
            mb.Write(ApiFunConst.SetTagRealAndHisValueTimerWithUserFun);
            mb.Write(this.mUser);
            mb.Write(this.mPass);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.TagName);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value,vv.Time, vv.Quality, mb);
            }
            SendData(mb);
            return IsConnected;
        }

        ///// <summary>
        ///// 异步设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        ///// 对于历史,记录类型为驱动时起作用
        ///// </summary>
        ///// <param name="data"></param>
        ///// <param name="timeout"></param>
        ///// <returns></returns>
        //public bool SetTagRealAndHisValueOneWay(RealDataBuffer data, int timeout = 5000)
        //{
        //    CheckLogin();
        //    if (data.Position <= 0 || mIsRealDataBusy) return false;
        //    var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position);
        //    mb.Write(ApiFunConst.SetTagRealAndHisValueFun);
        //    mb.Write(this.mLoginId);
        //    mb.Write(data.ValueCount);
        //    mb.Write(data.Buffers, (int)data.Position);
        //    SendData(mb);
        //    return true;
        //}

        ///// <summary>
        ///// 异步设置一组变量的实时同时对于驱动记录类型的变量,记录到历史
        ///// 对于历史,记录类型为驱动时起作用
        ///// 不用预先登录，每次请求均带着用户名密码、用于单向传输的情况
        ///// </summary>
        ///// <param name="data"></param>
        ///// <param name="timeout"></param>
        ///// <returns></returns>
        //public bool SetTagRealAndHisValueAsync2(RealDataBuffer data)
        //{
        //    if (data.Position <= 0 || mIsRealDataBusy) return false;
        //    var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position+64);
        //    mb.Write(ApiFunConst.SetTagRealAndHisValueWithUserFun);
        //    mb.Write(this.mUser);
        //    mb.Write(this.mPass);
        //    mb.Write(data.ValueCount);

        //    mb.Write(data.Buffers, (int)data.Position);
        //    SendData(mb);
        //    return true;
        //}



        /// <summary>
        /// 通过变量的ID，设置一组变量的带时间戳、质量戳的实时值
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValueTimerAndQuality(List<RealTagValueWithTimer> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 40 + 4);
            mb.Write(ApiFunConst.SetTagValueTimeAndQualityFun);
            mb.Write(this.mLoginId);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value,vv.Time, vv.Quality, mb);
            }
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();

                }

            }


            return false;
        }

        /// <summary>
        /// 通过变量的ID，设置一组变量的带时间戳、质量戳的实时值
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public void SetTagValueTimerAndQualityAsync(List<RealTagValueWithTimer> values, Action<bool> callback, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) callback?.Invoke(false);
            var vid = GetCallId();
            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 40 + 12);
            mb.Write(ApiFunConst.SetTagValueTimeAndQualityFunAsync);
            mb.Write(this.mLoginId);
            mb.Write(vid);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, vv.Time, vv.Quality, mb);
            }

            CallContext ctx = GetContext(vid, data => {
                if ((DateTime.Now - data.StartTime).TotalMilliseconds < data.Timeout)
                {
                    callback?.Invoke(data.Data?.ReadableCount > 0);
                }
                else
                {
                    callback?.Invoke(false);
                }

                lock (mCallBackDatas)
                {
                    if (mCallBackDatas.ContainsKey(data.Id))
                        mCallBackDatas.Remove(data.Id);
                }

                data.Data?.UnlockAndReturn();
                data.Data = null;
                data.CallBackAction = null;

            });
            ctx.StartTime = DateTime.Now;
            ctx.Timeout = timeout;
            lock (mCallBackDatas)
            {
                mCallBackDatas.Add(vid, ctx);
            }

            SendData(mb);

           
        }

        /// <summary>
        /// 通过变量的ID，设置一组变量的带时间戳、质量戳的实时值
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public void SetTagValueTimerAndQualityAsync(RealDataBuffer data, Action<bool> callback, int timeout = 5000)
        {
            CheckLogin();

            if (mIsRealDataBusy) callback?.Invoke(false);

            var vid = GetCallId();

            var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position + 12);
            mb.Write(ApiFunConst.SetTagValueTimeAndQualityFunAsync);
            mb.Write(this.mLoginId);
            mb.Write(vid);
            mb.Write(data.ValueCount);
            mb.Write(data.Buffers, (int)data.Position);

            CallContext ctx = GetContext(vid, data => {
                if ((DateTime.Now - data.StartTime).TotalMilliseconds < data.Timeout)
                {
                    callback?.Invoke(data.Data?.ReadableCount > 0);
                }
                else
                {
                    callback?.Invoke(false);
                }

                lock (mCallBackDatas)
                {
                    if (mCallBackDatas.ContainsKey(data.Id))
                        mCallBackDatas.Remove(data.Id);
                }

                data.Data?.UnlockAndReturn();
                data.Data = null;
                data.CallBackAction = null;

            });
            ctx.StartTime = DateTime.Now;
            ctx.Timeout = timeout;

            lock (mCallBackDatas)
            {
                mCallBackDatas.Add(vid, ctx);
            }

            SendData(mb);
        }


        ///// <summary>
        ///// 单向设置一组变量的实时值和质量戳，不等返回值
        ///// 立即返回
        ///// </summary>
        ///// <param name="data"></param>
        ///// <param name="timeout"></param>
        ///// <returns></returns>
        //public bool SetTagValueAndQualityNoWait(RealDataBuffer data, int timeout = 5000)
        //{
        //    CheckLogin();
        //    if (mIsRealDataBusy) return false;

        //    var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position);
        //    mb.Write(ApiFunConst.SetTagValueAndQualityFun);
        //    mb.Write(this.mLoginId);
        //    mb.Write(data.ValueCount);

        //    mb.Write(data.Buffers, (int)data.Position);
        //    //System.Runtime.InteropServices.Marshal.Copy(data.Buffers, mb.Array, mb.ArrayOffset + mb.WriterIndex, (int)data.Position);
        //    //mb.SetWriterIndex((int)(mb.WriterIndex + data.Position));

        //    realRequreEvent.Reset();
        //    SendData(mb);

        //    return true;
        //}

        ///// <summary>
        ///// 异步设置设置一组变量的实时值和质量戳
        ///// 用于数据单向传输
        ///// </summary>
        ///// <param name="data"></param>
        ///// <param name="timeout"></param>
        ///// <returns></returns>
        //public bool SetTagValueAndQualityAsync2(RealDataBuffer data)
        //{
        //    //CheckLogin();
        //    if (mIsRealDataBusy) return false;

        //    var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position+64);
        //    mb.Write(ApiFunConst.SetTagValueAndQualityWithUserFun);
        //    mb.Write(this.mUser);
        //    mb.Write(this.mPass);
        //    mb.Write(data.ValueCount);

        //    mb.Write(data.Buffers, (int)data.Position);

        //    //realRequreEvent.Reset();
        //    SendData(mb);

        //    return true;
        //}

        /// <summary>
        /// 设置一组变量的实时值和质量戳
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
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
                }
                finally
                {
                    mRealRequreData?.UnlockAndReturn();
                }
            }
            return false;
        }

        /// <summary>
        /// 通过变量的ID，设置变量的实时值
        /// </summary>
        /// <param name="values">ID，值类型，值，质量</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public bool SetTagValueAndQuality(List<RealTagValue> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 32 + 4);
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

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();

                }

            }


            return false;
        }

        /// <summary>
        /// 通过变量的ID，通过异步通讯的方式设置一组变量的带质量戳的实时值
        /// </summary>
        /// <param name="data">数据块</param>
        /// <param name="timeout">超时</param>
        /// <param name="callback">回调</param>
        /// <returns></returns>
        public void SetTagValueAndQualityAsync(RealDataBuffer data,Action<bool> callback, int timeout = 5000)
        {
            CheckLogin();

            if (mIsRealDataBusy)  callback?.Invoke(false);

            var vid = GetCallId();

            var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position+12);
            mb.Write(ApiFunConst.SetTagValueAndQualityFunAsync);
            mb.Write(this.mLoginId);
            mb.Write(vid);
            mb.Write(data.ValueCount);
            mb.Write(data.Buffers, (int)data.Position);

            CallContext ctx = GetContext(vid, data => {
                if ((DateTime.Now - data.StartTime).TotalMilliseconds < data.Timeout)
                {
                    callback?.Invoke(data.Data?.ReadableCount > 0);
                }
                else
                {
                    callback?.Invoke(false);
                }

                lock (mCallBackDatas)
                {
                    if (mCallBackDatas.ContainsKey(data.Id))
                        mCallBackDatas.Remove(data.Id);
                }

                data.Data?.UnlockAndReturn();
                data.Data = null;
                data.CallBackAction = null;

            });
            ctx.StartTime = DateTime.Now;
            ctx.Timeout = timeout;

            lock (mCallBackDatas)
            {
                mCallBackDatas.Add(vid, ctx);
            }

            SendData(mb);
        }


        /// <summary>
        /// 通过变量的ID，通过异步通讯的方式设置一组变量的带质量戳的实时值
        /// </summary>
        /// <param name="values">ID，值类型，值，质量</param>
        /// <param name="timeout">超时</param>
        /// <param name="callback">回调</param>
        /// <returns></returns>
        public void SetTagValueAndQualityAsync(List<RealTagValue> values,Action<bool> callback, int timeout = 5000)
        {
            CheckLogin();
            if (mIsRealDataBusy) callback?.Invoke(false);
            var vid = GetCallId();
            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * 32 + 12);
            mb.Write(ApiFunConst.SetTagValueAndQualityFunAsync);
            mb.Write(this.mLoginId);
            mb.Write(vid);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, vv.Quality, mb);
            }
           

            CallContext ctx = GetContext(vid, data => {
                if ((DateTime.Now - data.StartTime).TotalMilliseconds < data.Timeout)
                {
                    callback?.Invoke(data.Data?.ReadableCount > 0);
                }
                else
                {
                    callback?.Invoke(false);
                }

                lock (mCallBackDatas)
                {
                    if (mCallBackDatas.ContainsKey(data.Id))
                        mCallBackDatas.Remove(data.Id);
                }

                data.Data?.UnlockAndReturn();
                data.Data = null;
                data.CallBackAction = null;

            });
            ctx.StartTime = DateTime.Now;
            ctx.Timeout = timeout;
            lock (mCallBackDatas)
            {
                mCallBackDatas.Add(vid, ctx);
            }

            SendData(mb);
            
        }

        /// <summary>
        /// 单向 设置一组变量的带质量戳的实时值
        /// 不需要等待服务器返回响应，不需要预先登录，每次请求均带着用户名、密码，用于单向传输的情况,用于数据单向传输
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValueAndQuality2(RealDataBuffer data)
        {
            if (mIsRealDataBusy) return false;
            var mb = GetBuffer(ApiFunConst.RealValueFun, 14 + (int)data.Position + 64);
            mb.Write(ApiFunConst.SetTagValueAndQualityWithUserFun);
            mb.Write(this.mUser);
            mb.Write(this.mPass);
            mb.Write(data.ValueCount);
            mb.Write(data.Buffers, (int)data.Position);
            SendData(mb);
            return IsConnected;
        }


        /// <summary>
        /// 单向 设置一组变量的带质量戳的实时值
        /// 用于数据单向传输
        /// </summary>
        /// <param name="values">ID，值类型，值，质量</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValueAndQuality2(List<RealTagValue2> values)
        {
            if (mIsRealDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + values.Count * (32+64)+64);
            mb.Write(ApiFunConst.SetTagValueAndQualityWithUserFun);
            mb.Write(this.mUser);
            mb.Write(this.mPass);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer((TagType)vv.ValueType, vv.Value, vv.Quality, mb);
            }
            SendData(mb);
            return IsConnected;
        }

        

        /// <summary>
        /// 订购指定变量的值改变通知信息
        /// </summary>
        /// <param name="ids">Id集合</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool AppendRegistorDataChangedCallBack(IEnumerable<int> ids, int timeout = 5000)
        {
            CheckLogin();

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + ids.Count() * 4 + 4);
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
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
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

            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + ids.Count() * 4 + 4);
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
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
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
            var mb = GetBuffer(ApiFunConst.RealValueFun, 13 + 4);
            mb.Write(ApiFunConst.ClearRegistorTag);
            mb.Write(this.mLoginId);
            mb.Write(1);
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mRealRequreData != null && mRealRequreData.ReadableCount > 0;
                }
                catch
                {
                    mRealRequreData?.UnlockAndReturn();
                }

            }


            return false;
        }

        #endregion

        /// <summary>
        /// 设置区域相关的变量的值
        /// </summary>
        /// <param name="time">时间</param>
        /// <param name="values">值集合</param>
        /// <param name="timeout">超时时间</param>
        /// <returns></returns>
        public bool SetAreaTagHisValue(DateTime time, IEnumerable<RealTagValue> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsHisDataBusy) return false;
            var mb = GetBuffer(ApiFunConst.HisValueFun, 18 + values.Count() * 40 + 4);
            mb.Write(ApiFunConst.SetAreaTagHisValue);
            mb.Write(this.mLoginId);
            mb.Write(time); 
            mb.Write(values.Count());
            foreach (var vv in values)
            {
                mb.Write(vv.Id);
                SetTagValueToBuffer3((TagType)vv.ValueType, vv.Value, vv.Quality, mb);
            }
            hisRequreEvent.Reset();
            SendData(mb);

            if (this.hisRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 0;
                }
                catch
                {
                    mHisRequreData?.UnlockAndReturn();
                }

            }
            return false;
        }

        /// <summary>
        /// 设置区域相关的变量的值
        /// </summary>
        /// <param name="values">值</param>
        /// <param name="timeout">超时时间</param>
        /// <returns></returns>
        public bool SetAreaTagHisValue(Dictionary<DateTime,IEnumerable<RealTagValue>> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsHisDataBusy) return false;

            int count = 0;
            foreach(var vv in values)
            {
                count += vv.Value.Count();
            }

            var mb = GetBuffer(ApiFunConst.HisValueFun, 18 + count * 40 +values.Count*8+ 4);
            mb.Write(ApiFunConst.SetAreaTagHisValue2);
            mb.Write(this.mLoginId);
            mb.Write(values.Count);
            foreach (var vv in values)
            {
                mb.Write(vv.Key);
                mb.Write(vv.Value.Count());
                foreach (var vvv in vv.Value)
                {
                    mb.Write(vvv.Id);
                    SetTagValueToBuffer3((TagType)vvv.ValueType, vvv.Value, vvv.Quality, mb);
                }
            }
            hisRequreEvent.Reset();
            SendData(mb);

            if (this.hisRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 0;
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
        /// <param name="id">Id</param>
        /// <param name="type">值类型</param>
        /// <param name="values">值集合</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public bool SetTagHisValue(int id, TagType type, IEnumerable<TagValue> values, int timeout = 5000)
        {
            CheckLogin();
            if (mIsHisDataBusy) return false;
            var mb = GetBuffer(ApiFunConst.HisValueFun, 18 + values.Count() * 33 + 4);
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
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 0;
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
        /// 用于数据的单向传输
        /// </summary>
        /// <param name="tagName">Id</param>
        /// <param name="type">值类型</param>
        /// <param name="values">值集合</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public bool SetTagHisValue2(string tagName, TagType type, IEnumerable<TagValue> values, int timeout = 5000)
        {
            if (mIsHisDataBusy) return false;
            var mb = GetBuffer(ApiFunConst.HisValueFun, 18 + values.Count() * 33+128);
            mb.Write(ApiFunConst.SetTagHisValueWithUser);
            mb.Write(this.mUser);
            mb.Write(this.mPass);
            mb.Write(tagName);
            mb.Write(values.Count());
            mb.Write((byte)type);

            foreach (var vv in values)
            {
                mb.Write(vv.Time.ToBinary());
                SetTagValueToBuffer2(type, vv.Value, vv.Quality, mb);
            }
            SendData(mb);
            return IsConnected;
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

            var mb = GetBuffer(ApiFunConst.HisValueFun, 18 + (int)data.Position + 4);
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
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 0;
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
        /// 用于数据的单向传输
        /// </summary>
        /// <param name="id"></param>
        /// <param name="type"></param>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagHisValue2(string tagName, TagType type, HisDataBuffer data,int timeout=5000)
        {
            if (mIsHisDataBusy) return false;

            var mb = GetBuffer(ApiFunConst.HisValueFun, 18 + (int)data.Position+128);
            mb.Write(ApiFunConst.SetTagHisValueWithUser);
            mb.Write(this.mUser);
            mb.Write(this.mPass);
            mb.Write(tagName);
            mb.Write(data.ValueCount);
            mb.Write((byte)type);
            mb.Write(data.Buffers, (int)data.Position);
            SendData(mb);
            return IsConnected;
        }

        /// <summary>
        /// 设置一组变量的历史值
        /// </summary>
        /// <param name="idvalues">ID，值集合</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetMutiTagHisValue(Dictionary<int,IEnumerable<TagValueAndType>> idvalues,int timeout=5000)
        {
            if (idvalues == null && idvalues.Count == 0) return false;
            if (mIsHisDataBusy) return false;

            CheckLogin();
            int size = 0;

            foreach(var vv in idvalues)
            {
                size += (18 + vv.Value.Count() * 33);
            }

            var mb = GetBuffer(ApiFunConst.HisValueFun, 13 + size + 4);


            mb.Write(ApiFunConst.SetTagHisValue2);
            mb.Write(this.mLoginId);
            mb.Write(idvalues.Count);

            foreach (var vv in idvalues)
            {
                mb.Write(vv.Key);
                mb.Write(vv.Value.Count());
                mb.Write((byte)vv.Value.First().ValueType);

                foreach (var vvv in vv.Value)
                {
                    mb.Write(vvv.Time.ToBinary());
                    SetTagValueToBuffer2(vvv.ValueType, vvv.Value, vvv.Quality, mb);
                }
            }

            hisRequreEvent.Reset();
            SendData(mb);

            if (this.hisRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 0;
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
        /// 用于数据的单向传输
        /// </summary>
        /// <param name="idvalues">ID，值集合</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetMutiTagHisValue2(Dictionary<string, IEnumerable<TagValueAndType>> idvalues, int timeout = 5000)
        {
            if (idvalues == null && idvalues.Count == 0) return false;
            if (mIsHisDataBusy) return false;
            int size = 0;

            foreach (var vv in idvalues)
            {
                size += (18 + vv.Value.Count() * 33 + 128);
            }
            var mb = GetBuffer(ApiFunConst.HisValueFun, 128 + size + 4);
            mb.Write(ApiFunConst.SetTagHisValueWithUser2);
            mb.Write(this.mUser);
            mb.Write(this.mPass);
            mb.Write(idvalues.Count);

            foreach (var vv in idvalues)
            {
                mb.Write(vv.Key);
                mb.Write(vv.Value.Count());
                mb.Write((byte)vv.Value.First().ValueType);

                foreach (var vvv in vv.Value)
                {
                    mb.Write(vvv.Time.ToBinary());
                    SetTagValueToBuffer2(vvv.ValueType, vvv.Value, vvv.Quality, mb);
                }
            }
            SendData(mb);

            return IsConnected;
        }

        /// <summary>
        /// 设置一组变量的历史值
        /// </summary>
        /// <param name="idvalues">ID，值集合</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetMutiTagHisValue(HisDataBuffer data, int valuecount, int timeout = 5000)
        {
            if (data == null && valuecount == 0) return false;
            if (mIsHisDataBusy) return false;

            CheckLogin();
            var mb = GetBuffer(ApiFunConst.HisValueFun, 13 + (int)data.Position + 4);
            mb.Write(ApiFunConst.SetTagHisValue2);
            mb.Write(this.mLoginId);
            mb.Write(valuecount);
            mb.Write(data.Buffers, (int)data.Position);

            hisRequreEvent.Reset();
            SendData(mb);

            if (this.hisRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 0;
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                }
            }
            return false;
        }


        /// <summary>
        /// 设置一组变量的历史值
        /// 用于单项传输
        /// </summary>
        /// <param name="idvalues">ID，值集合</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetMutiTagHisValue2(HisDataBuffer data, int valuecount, int timeout = 5000)
        {
            if (data == null && valuecount == 0) return false;
            if (mIsHisDataBusy) return false;

            CheckLogin();
            var mb = GetBuffer(ApiFunConst.HisValueFun, 128 + (int)data.Position + 4);
            mb.Write(ApiFunConst.SetTagHisValueWithUser2);
            mb.Write(this.mUser);
            mb.Write(this.mPass);
            mb.Write(valuecount);
            mb.Write(data.Buffers, (int)data.Position);

            hisRequreEvent.Reset();
            SendData(mb);

            if (this.hisRequreEvent.WaitOne(timeout))
            {
                try
                {
                    return mHisRequreData != null && mHisRequreData.ReadableCount > 0;
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                }
            }
            return false;
        }


        public string GetDatabseName(int timeout = 5000)
        {
            lock (mTagInfoLockObj)
            {

                CheckLogin();
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 9 + 4);
                mb.Write(ApiFunConst.GetDatabaseName);
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
                                if (cmd == ApiFunConst.GetDatabaseName)
                                {
                                    return vdata.ReadString();
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
                return string.Empty;
            }
        }

        /// <summary>
        /// 获取变量的ID
        /// </summary>
        /// <param name="tags">变量名称的集合</param>
        /// <param name="timeout"></param>
        /// <returns>ID 集合</returns>
        public List<int> QueryTagId(IEnumerable<string> tags,int timeout = 5000)
        {
            lock (mTagInfoLockObj)
            {
                mInfoRequreData.Clear();
                List<int> re = new List<int>();
                CheckLogin();
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 13 + tags.Count() * 256 + 4);
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
                        while (mInfoRequreData.Count > 0)
                        {
                            var vdata = mInfoRequreData.Dequeue();
                            if (vdata != null)
                            {
                                var cid = vdata.ReadByte();
                                if (cid == ApiFunConst.GetTagIdByNameFun)
                                {
                                    var count = vdata.ReadInt();
                                    for (int i = 0; i < count; i++)
                                    {
                                        re.Add(vdata.ReadInt());
                                    }
                                    vdata.UnlockAndReturn();
                                    return re;
                                }
                                vdata.UnlockAndReturn();
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
        /// 通知使用的变量,用于单向传输时，同时网络中断，服务器知道哪些变量的质量戳设置成坏值
        /// </summary>
        /// <param name="tags">变量名称</param>
        /// <param name="timeout">超时</param>
        public void NotifyTags(IEnumerable<string> tags,int timeout=5000)
        {
            lock (mTagInfoLockObj)
            {             
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 13 + tags.Count() * 256 + 4);
                mb.Write(ApiFunConst.GetTagIdByNameFun);
                mb.Write(this.mLoginId);
                mb.Write(tags.Count());
                foreach (var vv in tags)
                {
                    mb.Write(vv);
                }
                infoRequreEvent.Reset();
                SendData(mb);
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
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 9 + 4);
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
                                        //if(id==0)
                                        //{
                                        //    Debug.Print("数据错误:" + id);
                                        //}
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
        /// 通过寄存器关键字查询变量
        /// </summary>
        /// <param name="filter">过滤条件</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public Dictionary<int, Tuple<string, byte>> QueryTagIdAndNamesbyFilter(string filter, int timeout = 5000)
        {
            lock (mTagInfoLockObj)
            {
                Dictionary<int, Tuple<string, byte>> re = new Dictionary<int, Tuple<string, byte>>();
                CheckLogin();
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 9+256 + 4);
                mb.Write(ApiFunConst.GetTagIdByFilterRegistor);
                mb.Write(this.mLoginId);
                mb.Write(filter);

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
                                if (cmd == ApiFunConst.QueryAllTagNameAndIds)
                                {
                                    int total = vdata.ReadShort();
                                    int icount = vdata.ReadShort();
                                    int tcount = vdata.ReadInt();
                                    string name = "";
                                    for (int i = 0; i < tcount; i++)
                                    {
                                        var id = vdata.ReadInt();
                                        if (id == 0)
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
        /// 获取所有历史记录类型为驱动的变量的ID
        /// </summary>
        /// <param name="timeout">超时</param>
        /// <returns>变量ID集合</returns>
        public List<int> GetDriverRecordTypeTagIds(int timeout = 5000)
        {
            lock (mTagInfoLockObj)
            {
                List<int> re = new List<int>();
                CheckLogin();
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 9 + 4);
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
        /// <param name="ids">Id集合</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public List<bool> CheckRecordTypeByTagId(IEnumerable<int> ids, int timeout = 5000)
        {
            lock (mTagInfoLockObj)
            {
                List<bool> re = new List<bool>();
                CheckLogin();
                var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 13 + ids.Count() * 4 + 4);
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

        #region Read Tag Real Value

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private int GetRealRequreId()
        {
            lock (mHisDataLock)
            {
                var vid = mHisRequreCount;
                mHisRequreCount++;
                return vid;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="callback"></param>
        private void RegistorRealDataCallback(int id, Action<ByteBuffer> callback)
        {
            lock (mRealDataLock)
            {
                mRealDataCallBack.Add(id, callback);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="callback"></param>
        private void UnRegistorRealDataCallback(int id)
        {
            lock (mRealDataLock)
            {
                if (mRealDataCallBack.ContainsKey(id))
                {
                    mRealDataCallBack.Remove(id);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="vid"></param>
        /// <param name="mb"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private ByteBuffer SendAndWait(int vid, ByteBuffer mb, int timeout)
        {
            ByteBuffer re = null;
            ManualResetEvent mGetRealDataCallBackEvent = new ManualResetEvent(false);
            try
            {
                RegistorRealDataCallback(vid, (data) =>
                {
                    re = data;
                    if (!mGetRealDataCallBackEvent.SafeWaitHandle.IsClosed)
                        mGetRealDataCallBackEvent.Set();
                    else
                    {
                        data?.UnlockAndReturn();
                    }
                });
                SendData(mb);

                if (mGetRealDataCallBackEvent.WaitOne(timeout))
                {
                    return re;
                }
                else
                {
                    re?.UnlockAndReturn();
                }
            }
            finally
            {
                UnRegistorRealDataCallback(vid);
                mGetRealDataCallBackEvent.Dispose();
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private ByteBuffer GetRealDataInner(List<int> ids, int timeout = 5000)
        {
            lock (mRealDataLock)
            {

                var vid = GetRealRequreId();

                var mb = GetBuffer(ApiFunConst.RealValueFun, 8 + ids.Count * 4 + 4);
                mb.Write(ApiFunConst.ReadTagRealValue);
                mb.Write(this.LoginId);
                mb.Write(vid);

                mb.Write(ids.Count);

                for (int i = 0; i < ids.Count; i++)
                {
                    mb.Write(ids[i]);
                }
                return SendAndWait(vid, mb, timeout);
            }
        }

        /// <summary>
        /// 获取变量的实时值
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int, TagRealValue> GetRealData(List<int> ids,int timeout=5000)
        {
            Dictionary<int, TagRealValue> re = new Dictionary<int, TagRealValue>();
            var vdata = GetRealDataInner(ids, timeout);
            object val=null;
            if (vdata != null)
            {
                var valuecount = vdata.ReadInt();
                for (int i = 0; i < valuecount; i++)
                {
                    var vid = vdata.ReadInt();
                    var type = vdata.ReadByte();

                    switch (type)
                    {
                        case (byte)TagType.Bool:
                            val = ((byte)vdata.ReadByte());
                            break;
                        case (byte)TagType.Byte:
                            val = ((byte)vdata.ReadByte());
                            break;
                        case (byte)TagType.Short:
                            val = (vdata.ReadShort());
                            break;
                        case (byte)TagType.UShort:
                            val = (vdata.ReadUShort());
                            break;
                        case (byte)TagType.Int:
                            val = (vdata.ReadInt());
                            break;
                        case (byte)TagType.UInt:
                            val = (vdata.ReadUInt());
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            val = (vdata.ReadULong());
                            break;
                        case (byte)TagType.Float:
                            val = (vdata.ReadFloat());
                            break;
                        case (byte)TagType.Double:
                            val = (vdata.ReadDouble());
                            break;
                        case (byte)TagType.String:
                            val = vdata.ReadString();
                            break;
                        case (byte)TagType.DateTime:
                            val = DateTime.FromBinary(vdata.ReadLong());
                            break;
                        case (byte)TagType.IntPoint:
                            val = new IntPointData(vdata.ReadInt(), vdata.ReadInt());
                            break;
                        case (byte)TagType.UIntPoint:
                            val = new UIntPointData(vdata.ReadInt(), vdata.ReadInt());
                            break;
                        case (byte)TagType.IntPoint3:
                            val = new IntPoint3Data(vdata.ReadInt(), vdata.ReadInt(), vdata.ReadInt());
                            break;
                        case (byte)TagType.UIntPoint3:
                            val = new UIntPoint3Data(vdata.ReadInt(), vdata.ReadInt(), vdata.ReadInt());
                            break;
                        case (byte)TagType.LongPoint:
                            val = new LongPointData(vdata.ReadLong(), vdata.ReadLong());
                            break;
                        case (byte)TagType.ULongPoint:
                            val = new ULongPointData(vdata.ReadLong(), vdata.ReadLong());
                            break;
                        case (byte)TagType.LongPoint3:
                            val = new LongPoint3Data(vdata.ReadLong(), vdata.ReadLong(), vdata.ReadLong());
                            break;
                        case (byte)TagType.ULongPoint3:
                            val = new ULongPoint3Data(vdata.ReadLong(), vdata.ReadLong(), vdata.ReadLong());
                            break;
                        case (byte)TagType.Complex:
                            ComplexRealValue cv =new ComplexRealValue();
                            int count = vdata.ReadInt();
                            for(int j=0;j<count;j++)
                            {
                                var cvid = vdata.ReadInt();
                                var vtp = vdata.ReadByte();
                                object cval = null;
                                switch(vtp)
                                {
                                    case (byte)TagType.Bool:
                                        cval = ((byte)vdata.ReadByte());
                                        break;
                                    case (byte)TagType.Byte:
                                        cval = ((byte)vdata.ReadByte());
                                        break;
                                    case (byte)TagType.Short:
                                        cval = (vdata.ReadShort());
                                        break;
                                    case (byte)TagType.UShort:
                                        cval = (vdata.ReadUShort());
                                        break;
                                    case (byte)TagType.Int:
                                        cval = (vdata.ReadInt());
                                        break;
                                    case (byte)TagType.UInt:
                                        cval = (vdata.ReadUInt());
                                        break;
                                    case (byte)TagType.Long:
                                    case (byte)TagType.ULong:
                                        cval = (vdata.ReadULong());
                                        break;
                                    case (byte)TagType.Float:
                                        cval = (vdata.ReadFloat());
                                        break;
                                    case (byte)TagType.Double:
                                        cval = (vdata.ReadDouble());
                                        break;
                                    case (byte)TagType.String:
                                        cval = vdata.ReadString();
                                        break;
                                    case (byte)TagType.DateTime:
                                        cval = DateTime.FromBinary(vdata.ReadLong());
                                        break;
                                    case (byte)TagType.IntPoint:
                                        cval = new IntPointData(vdata.ReadInt(), vdata.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint:
                                        cval = new UIntPointData(vdata.ReadInt(), vdata.ReadInt());
                                        break;
                                    case (byte)TagType.IntPoint3:
                                        cval = new IntPoint3Data(vdata.ReadInt(), vdata.ReadInt(), vdata.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint3:
                                        cval = new UIntPoint3Data(vdata.ReadInt(), vdata.ReadInt(), vdata.ReadInt());
                                        break;
                                    case (byte)TagType.LongPoint:
                                        cval = new LongPointData(vdata.ReadLong(), vdata.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint:
                                        cval = new ULongPointData(vdata.ReadLong(), vdata.ReadLong());
                                        break;
                                    case (byte)TagType.LongPoint3:
                                        cval = new LongPoint3Data(vdata.ReadLong(), vdata.ReadLong(), vdata.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint3:
                                        cval = new ULongPoint3Data(vdata.ReadLong(), vdata.ReadLong(), vdata.ReadLong());
                                        break;
                                }
                                var ctime = DateTime.FromBinary(vdata.ReadLong()).ToLocalTime();
                                var cqua = vdata.ReadByte();
                                cv.Add(new ComplexRealValueItem() { Id=cvid,ValueType=vtp,Value=cval,Quality=cqua,Time=ctime});
                            }
                            val = cv;
                            break;
                    }
                    var vtime = DateTime.FromBinary(vdata.ReadLong()).ToLocalTime();
                    var qua = vdata.ReadByte();

                    if (!re.ContainsKey(vid))
                        re.Add(vid, new TagRealValue() { Value = val, Quality = qua, Time = vtime });

                }
                vdata.ReleaseToPools();
            }
            return re;
        }

        #endregion

        #region Read His Value

        /// <summary>
        /// 根据SQL查询历史数据
        /// </summary>
        /// <param name="sql">Sql 语句</param>
        /// <param name="timeout"></param>
        /// <returns>HisQueryTableResult 历史数据 或者List<double> 统计值或者Dictionary<int, TagRealValue> 实时值</returns>
        public object QueryHisValueBySql(string sql, int timeout = 5000)
        {
            ByteBuffer re = null;
            var vid = 0;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
            }
            var mb = GetBuffer(ApiFunConst.HisValueFun, 8 + sql.Length + 4);
            mb.Write(ApiFunConst.ReadHisValueBySQL);
            mb.Write(this.LoginId);
            mb.Write(sql);
            mb.Write(vid);
            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) => {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }

            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    var byt = re.ReadByte();
                    if(ApiFunConst.ReadHisValueBySQL == byt)
                    {
                        var typ = re.ReadByte();
                        if(typ == 0)
                        {
                            //历史值
                            Cdy.Tag.HisQueryTableResult rel = new HisQueryTableResult();
                            string smeta = re.ReadString();
                            rel.FromStringToMeta(smeta);
                            int dsize = re.ReadInt();
                            rel.Init2(dsize);
                            re.CopyTo(rel.Address, re.ReadIndex, 0, dsize);
                            return rel;
                        }
                        else if(typ==1)
                        {
                            //统计值
                            List<double> ltmp = new List<double>();
                            int dsize =re.ReadInt();
                            for(int i = 0; i < dsize; i++)
                            {
                                ltmp.Add(re.ReadDouble());
                            }
                            return ltmp;
                        }
                        else if(typ==2)
                        {
                            //实时值
                            object val = null;
                            Dictionary<int, TagRealValue> rre = new Dictionary<int, TagRealValue>();
                            var valuecount = re.ReadInt();
                            for (int i = 0; i < valuecount; i++)
                            {
                                var vvid = re.ReadInt();
                                var type = re.ReadByte();

                                switch (type)
                                {
                                    case (byte)TagType.Bool:
                                        val = ((byte)re.ReadByte());
                                        break;
                                    case (byte)TagType.Byte:
                                        val = ((byte)re.ReadByte());
                                        break;
                                    case (byte)TagType.Short:
                                        val = (re.ReadShort());
                                        break;
                                    case (byte)TagType.UShort:
                                        val = (re.ReadUShort());
                                        break;
                                    case (byte)TagType.Int:
                                        val = (re.ReadInt());
                                        break;
                                    case (byte)TagType.UInt:
                                        val = (re.ReadUInt());
                                        break;
                                    case (byte)TagType.Long:
                                    case (byte)TagType.ULong:
                                        val = (re.ReadULong());
                                        break;
                                    case (byte)TagType.Float:
                                        val = (re.ReadFloat());
                                        break;
                                    case (byte)TagType.Double:
                                        val = (re.ReadDouble());
                                        break;
                                    case (byte)TagType.String:
                                        val = re.ReadString();
                                        break;
                                    case (byte)TagType.DateTime:
                                        val = DateTime.FromBinary(re.ReadLong());
                                        break;
                                    case (byte)TagType.IntPoint:
                                        val = new IntPointData(re.ReadInt(), re.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint:
                                        val = new UIntPointData(re.ReadInt(), re.ReadInt());
                                        break;
                                    case (byte)TagType.IntPoint3:
                                        val = new IntPoint3Data(re.ReadInt(), re.ReadInt(), re.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint3:
                                        val = new UIntPoint3Data(re.ReadInt(), re.ReadInt(), re.ReadInt());
                                        break;
                                    case (byte)TagType.LongPoint:
                                        val = new LongPointData(re.ReadLong(), re.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint:
                                        val = new ULongPointData(re.ReadLong(), re.ReadLong());
                                        break;
                                    case (byte)TagType.LongPoint3:
                                        val = new LongPoint3Data(re.ReadLong(), re.ReadLong(), re.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint3:
                                        val = new ULongPoint3Data(re.ReadLong(), re.ReadLong(), re.ReadLong());
                                        break;
                                    case (byte)TagType.Complex:
                                        ComplexRealValue cv = new ComplexRealValue();
                                        int count = re.ReadInt();
                                        for (int j = 0; j < count; j++)
                                        {
                                            var cvid = re.ReadInt();
                                            var vtp = re.ReadByte();
                                            object cval = null;
                                            switch (vtp)
                                            {
                                                case (byte)TagType.Bool:
                                                    cval = ((byte)re.ReadByte());
                                                    break;
                                                case (byte)TagType.Byte:
                                                    cval = ((byte)re.ReadByte());
                                                    break;
                                                case (byte)TagType.Short:
                                                    cval = (re.ReadShort());
                                                    break;
                                                case (byte)TagType.UShort:
                                                    cval = (re.ReadUShort());
                                                    break;
                                                case (byte)TagType.Int:
                                                    cval = (re.ReadInt());
                                                    break;
                                                case (byte)TagType.UInt:
                                                    cval = (re.ReadUInt());
                                                    break;
                                                case (byte)TagType.Long:
                                                case (byte)TagType.ULong:
                                                    cval = (re.ReadULong());
                                                    break;
                                                case (byte)TagType.Float:
                                                    cval = (re.ReadFloat());
                                                    break;
                                                case (byte)TagType.Double:
                                                    cval = (re.ReadDouble());
                                                    break;
                                                case (byte)TagType.String:
                                                    cval = re.ReadString();
                                                    break;
                                                case (byte)TagType.DateTime:
                                                    cval = DateTime.FromBinary(re.ReadLong());
                                                    break;
                                                case (byte)TagType.IntPoint:
                                                    cval = new IntPointData(re.ReadInt(), re.ReadInt());
                                                    break;
                                                case (byte)TagType.UIntPoint:
                                                    cval = new UIntPointData(re.ReadInt(), re.ReadInt());
                                                    break;
                                                case (byte)TagType.IntPoint3:
                                                    cval = new IntPoint3Data(re.ReadInt(), re.ReadInt(), re.ReadInt());
                                                    break;
                                                case (byte)TagType.UIntPoint3:
                                                    cval = new UIntPoint3Data(re.ReadInt(), re.ReadInt(), re.ReadInt());
                                                    break;
                                                case (byte)TagType.LongPoint:
                                                    cval = new LongPointData(re.ReadLong(), re.ReadLong());
                                                    break;
                                                case (byte)TagType.ULongPoint:
                                                    cval = new ULongPointData(re.ReadLong(), re.ReadLong());
                                                    break;
                                                case (byte)TagType.LongPoint3:
                                                    cval = new LongPoint3Data(re.ReadLong(), re.ReadLong(), re.ReadLong());
                                                    break;
                                                case (byte)TagType.ULongPoint3:
                                                    cval = new ULongPoint3Data(re.ReadLong(), re.ReadLong(), re.ReadLong());
                                                    break;
                                            }
                                            var ctime = DateTime.FromBinary(re.ReadLong()).ToLocalTime();
                                            var cqua = re.ReadByte();
                                            cv.Add(new ComplexRealValueItem() { Id = cvid, ValueType = vtp, Value = cval, Quality = cqua, Time = ctime });
                                        }
                                        val = cv;
                                        break;
                                }
                                var vtime = DateTime.FromBinary(re.ReadLong()).ToLocalTime();
                                var qua = re.ReadByte();

                                if (!rre.ContainsKey(vid))
                                    rre.Add(vid, new TagRealValue() { Value = val, Quality = qua, Time = vtime });
                            }
                            return rre;
                        }
                    }
                    return null;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                    }

                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }
        /// <summary>
        /// 查询变量某个时间段的记录的所有值
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <returns></returns>
        private ByteBuffer QueryAllHisValueInner(int id, DateTime startTime, DateTime endTime, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
            }
            var mb = GetBuffer(ApiFunConst.HisValueFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.ReadTagAllHisValue);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) => {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }

            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }

                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 查询变量某个时间段的记录的所有值
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="startTime">开始时间(UniversalTime)</param>
        /// <param name="endTime">结束时间(UniversalTime)</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryAllHisValue<T>(int id, DateTime startTime, DateTime endTime, int timeout = 5000)
        {
            ByteBuffer mb = QueryAllHisValueInner(id, startTime, endTime, timeout);
            if (mb != null)
                return ProcessHisResultByMemory<T>(mb);
            else
                return null;
        }

        /// <summary>
        /// 以本地时间查询变量某个时间段的记录的所有值
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="startTime">开始时间(LocalTime)</param>
        /// <param name="endTime">结束时间(LocalTime)</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryAllHisValueByLocalTime<T>(int id, DateTime startTime, DateTime endTime, int timeout = 5000)
        {
            ByteBuffer mb = QueryAllHisValueInner(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), timeout);
            if (mb != null)
                return ProcessHisResultByMemory<T>(mb);
            else
                return null;
        }

        /// <summary>
        /// 查询某个变量在一系列时间点的上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="times">时间点集合</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <returns></returns>
        private ByteBuffer QueryHisValueAtTimesInner(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
            }

            var mb = GetBuffer(ApiFunConst.HisValueFun, 8 + times.Count * 8 + 5 + 4);
            mb.Write(ApiFunConst.ReadHisDatasByTimePoint);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write((byte)matchType);
            mb.Write(times.Count);
            for (int i = 0; i < times.Count; i++)
            {
                mb.Write(times[i].Ticks);
            }
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();
            try
            {
                lock (mHisDataCallBack)
                {
                    mHisDataCallBack.Add(vid, (data) =>
                    {
                        re = data;
                        try
                        {
                            hisRequreEvent.Set();
                        }
                        catch
                        {
                            data?.UnlockAndReturn();
                        }
                    });
                }
                SendData(mb);
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }
                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 查询某个变量在一系列时间点的上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="times">时间点集合(UniversalTimes)</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueAtTimes<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            ByteBuffer mb = QueryHisValueAtTimesInner(id, times, matchType, timeout);
            if (mb != null)
                return ProcessHisResultByMemory<T>(mb);
            else
                return null;
        }


        /// <summary>
        /// 以本地时间查询某个变量在一系列时间点的上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="times">时间点集合(LocalTimes)</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueAtTimesByLocalTimes<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            ByteBuffer mb = QueryHisValueAtTimesInner(id, times.Select(e=>e.ToUniversalTime()).ToList(), matchType, timeout);
            if (mb != null)
                return ProcessHisResultByMemory<T>(mb);
            else
                return null;
        }

        /// <summary>
        /// 查询某个变量在一系列时间点的上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// 忽略系统退出对拟合的影响
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="times">时间点集合</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        private ByteBuffer QueryHisValueAtTimesByIgnorSystemExitInner(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
            }

            var mb = GetBuffer(ApiFunConst.HisValueFun, 8 + times.Count * 8 + 5 + 4);
            mb.Write(ApiFunConst.ReadHisDatasByTimePointByIgnorClosedQuality);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write((byte)matchType);
            mb.Write(times.Count);
            for (int i = 0; i < times.Count; i++)
            {
                mb.Write(times[i].Ticks);
            }
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();
            try
            {
                lock (mHisDataCallBack)
                {
                    mHisDataCallBack.Add(vid, (data) =>
                    {
                        re = data;
                        try
                        {
                            hisRequreEvent.Set();
                        }
                        catch
                        {
                            data?.UnlockAndReturn();
                        }
                    });
                }
                SendData(mb);
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }
                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }


        /// <summary>
        /// 查询某个变量在一系列时间点的上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// 忽略系统退出对拟合的影响
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="times">时间点集合(UniversalTimes)</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueAtTimesByIgnorSystemExit<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            ByteBuffer mb = QueryHisValueAtTimesByIgnorSystemExitInner(id, times, matchType, timeout);
            if (mb != null)
                return ProcessHisResultByMemory<T>(mb);
            else
                return null;
        }


        /// <summary>
        /// 以本地时间查询某个变量在一系列时间点的上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// 忽略系统退出对拟合的影响
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="times">时间点集合(LocalTimes)</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueAtTimesByIgnorSystemExitByLocalTime<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            ByteBuffer mb = QueryHisValueAtTimesByIgnorSystemExitInner(id, times.Select(e=>e.ToUniversalTime()).ToList(), matchType, timeout);
            if (mb != null)
                return ProcessHisResultByMemory<T>(mb);
            else
                return null;
        }

        /// <summary>
        /// 查询某个变量在某个时间段内以指定时间间隔点上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="span"></param>
        /// <param name="matchType"></param>
        /// <returns></returns>
        private ByteBuffer QueryHisValueForTimeSpanInner(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
            }

            var mb = GetBuffer(ApiFunConst.HisValueFun, 8 + 24 + 5 + 4);
            mb.Write(ApiFunConst.ReadHisDataByTimeSpan);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write((byte)matchType);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(span.Ticks);
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();
            try
            {
                lock (mHisDataCallBack)
                {
                    mHisDataCallBack.Add(vid, (data) =>
                    {
                        re = data;
                        try
                        {
                            hisRequreEvent.Set();
                        }
                        catch
                        {
                            data?.UnlockAndReturn();
                        }
                    });
                }
                SendData(mb);
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }
                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 查询某个变量在某个时间段内以指定时间间隔点上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime">开始时间(UniversalTime)</param>
        /// <param name="endTime">结束时间(UniversalTime)</param>
        /// <param name="span">间隔</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueForTimeSpan<T>(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            ByteBuffer mb = QueryHisValueForTimeSpanInner(id, startTime,endTime, span,matchType, timeout);
            if (mb != null)
                return ProcessHisResultByMemory<T>(mb);
            else
                return null;
        }

        /// <summary>
        /// 以本地时间查询某个变量在某个时间段内以指定时间间隔点上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime">开始时间(LocalTime)</param>
        /// <param name="endTime">结束时间(LocalTime)</param>
        /// <param name="span">间隔</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueForTimeSpanByLocalTime<T>(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            ByteBuffer mb = QueryHisValueForTimeSpanInner(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), span, matchType, timeout);
            if (mb != null)
                return ProcessHisResultByMemory<T>(mb);
            else
                return null;
        }

        /// <summary>
        /// 查询某个变量在某个时间段内以指定时间间隔点上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="span">间隔</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        private ByteBuffer QueryHisValueForTimeSpanByIgnorSystemExitInner(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
            }

            var mb = GetBuffer(ApiFunConst.HisValueFun, 8 + 24 + 5 + 4);
            mb.Write(ApiFunConst.ReadHisDataByTimeSpanByIgnorClosedQuality);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write((byte)matchType);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(span.Ticks);
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();
            try
            {
                lock (mHisDataCallBack)
                {
                    mHisDataCallBack.Add(vid, (data) =>
                    {
                        re = data;
                        try
                        {
                            hisRequreEvent.Set();
                        }
                        catch
                        {
                            data?.UnlockAndReturn();
                        }
                    });
                }
                SendData(mb);
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }
                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 查询某个变量在某个时间段内以指定时间间隔点上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime">开始时间(UniversalTime)</param>
        /// <param name="endTime">结束时间(UniversalTime)</param>
        /// <param name="span">间隔</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueForTimeSpanByIgnorSystemExit<T>(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            ByteBuffer mb = QueryHisValueForTimeSpanByIgnorSystemExitInner(id, startTime, endTime, span, matchType, timeout);
            if (mb != null)
                return ProcessHisResultByMemory<T>(mb);
            else
                return null;
        }

        /// <summary>
        /// 以本地时间查询某个变量在某个时间段内以指定时间间隔点上的值，如果没有记录则采用数据拟合的方式进行拟合
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime">开始时间(LocalTime)</param>
        /// <param name="endTime">结束时间(LocalTime)</param>
        /// <param name="span">间隔</param>
        /// <param name="matchType">拟合方式<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueForTimeSpanByIgnorSystemExitByLocalTime<T>(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            ByteBuffer mb = QueryHisValueForTimeSpanByIgnorSystemExitInner(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), span, matchType, timeout);
            if (mb != null)
                return ProcessHisResultByMemory<T>(mb);
            else
                return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data"></param>
        /// <param name="tp"></param>
        /// <returns></returns>
        private unsafe HisQueryResult<T> ProcessHisResultByMemory<T>(ByteBuffer data)
        {
            TagType tp = (TagType)data.ReadByte();
            int count = data.ReadInt();
            HisQueryResult<T> re = new HisQueryResult<T>(count);

            switch (tp)
            {
                case Cdy.Tag.TagType.Bool:
                    re = new HisQueryResult<bool>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.Byte:
                    re = new HisQueryResult<byte>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.DateTime:
                    re = new HisQueryResult<DateTime>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.Double:
                    re = new HisQueryResult<double>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.Float:
                    re = new HisQueryResult<float>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.Int:
                    re = new HisQueryResult<int>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.Long:
                    re = new HisQueryResult<long>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.Short:
                    re = new HisQueryResult<short>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.String:
                    re = new HisQueryResult<string>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.UInt:
                    re = new HisQueryResult<uint>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.ULong:
                    re = new HisQueryResult<ulong>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.UShort:
                    re = new HisQueryResult<ushort>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    re = new HisQueryResult<IntPointData>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    re = new HisQueryResult<UIntPointData>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    re = new HisQueryResult<IntPoint3Data>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    re = new HisQueryResult<UIntPoint3Data>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    re = new HisQueryResult<LongPointData>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    re = new HisQueryResult<ULongPointTag>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    re = new HisQueryResult<LongPoint3Data>(count) as HisQueryResult<T>;
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    re = new HisQueryResult<ULongPoint3Data>(count) as HisQueryResult<T>;
                    break;
            }

          
            data.CopyTo(re.Address, data.ReadIndex, 0, data.WriteIndex - data.ReadIndex);
            re.Count = count;
            data.UnlockAndReturn();
            return re;
        }

        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    ///// <summary>
    ///// 变量的实时值
    ///// </summary>
    //public struct TagRealValue
    //{
    //    /// <summary>
    //    /// 值
    //    /// </summary>
    //    public object Value { get; set; }
        
    //    /// <summary>
    //    /// 质量戳
    //    /// </summary>
    //    public byte Quality { get; set; }

    //    /// <summary>
    //    /// 时间
    //    /// </summary>
    //    public DateTime Time { get; set; }
    //}

    ///// <summary>
    ///// 
    ///// </summary>
    //public class ComplexRealValue : List<ComplexRealValueItem>
    //{
    //}


    ///// <summary>
    ///// 
    ///// </summary>
    //public struct ComplexRealValueItem
    //{
    //    public int Id { get; set; }

    //    /// <summary>
    //    /// 
    //    /// </summary>
    //    public byte ValueType { get; set; }

    //    /// <summary>
    //    /// 值
    //    /// </summary>
    //    public object Value { get; set; }

    //    /// <summary>
    //    /// 质量戳
    //    /// </summary>
    //    public byte Quality { get; set; }

    //    /// <summary>
    //    /// 时间
    //    /// </summary>
    //    public DateTime Time { get; set; }

    //}



    /// <summary>
    /// 
    /// </summary>
    public class CallContext
    {
        public long Id { get; set; }
        public ByteBuffer Data { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Timeout { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Action<CallContext> CallBackAction { get; set; }
    }
}
