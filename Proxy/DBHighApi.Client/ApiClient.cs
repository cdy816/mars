using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Cdy.Tag;
using DotNetty.Buffers;

namespace DBHighApi
{
    public class ApiFunConst
    {
        /// <summary>
        /// 
        /// </summary>
        public const byte TagInfoRequest = 1;

        public const byte GetTagIdByNameFun = 0;

        public const byte Login = 1;

        public const byte RegistorValueCallback = 2;


        public const byte GetRunnerDatabase = 3;

        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataRequestFun = 10;

        /// <summary>
        /// 获取实时值
        /// </summary>
        public const byte RequestRealData = 0;


        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealDataValue = 7;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealDataValueAndQuality = 8;


        /// <summary>
        /// 
        /// </summary>
        public const byte RequestReal2DataValue = 17;

        /// <summary>
        /// 
        /// </summary>

        public const byte RequestReal2DataValueAndQuality = 18;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealData2 = 10;

        /// <summary>
        /// 请求所有数据
        /// </summary>
        public const byte RealMemorySync = 13;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealDataByMemoryCopy = 11;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealData2ByMemoryCopy = 12;

        /// <summary>
        /// 设置实时值
        /// </summary>
        public const byte SetDataValue = 1;

        /// <summary>
        /// 值改变通知
        /// </summary>
        public const byte ValueChangeNotify = 2;

        /// <summary>
        /// 清空值改变通知
        /// </summary>
        public const byte ResetValueChangeNotify = 3;


        /// <summary>
        /// 块改变通知
        /// </summary>
        public const byte BlockValueChangeNotify = 4;


        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataPushFun = 12;


        /// <summary>
        /// 
        /// </summary>
        public const byte HisDataRequestFun = 20;


        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDatasByTimePoint = 0;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestAllHisData = 1;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDataByTimeSpan = 2;


        /// <summary>
        /// 
        /// </summary>
        public const byte RequestStatisticData = 3;

        /// <summary>
        /// 
        /// </summary>

        public const byte RequestStatisticDataByTimeSpan = 4;
    }

    public class ApiClient:SocketClient
    {

        #region ... Variables  ...
        
        private ManualResetEvent infoRequreEvent = new ManualResetEvent(false);

        private IByteBuffer mInfoRequreData;

        private ManualResetEvent hisRequreEvent = new ManualResetEvent(false);

        private IByteBuffer mHisRequreData;

        private ManualResetEvent realRequreEvent = new ManualResetEvent(false);

        private IByteBuffer mRealRequreData;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<byte, IByteBuffer> mReceivedDatas = new Dictionary<byte, IByteBuffer>();

        public delegate void TagValueChangedCallBackDelegate(Dictionary<int, Tuple<object, byte>> datas);

        private object mlockRealObj = new object();

        private object mlockRealSetObj = new object();


        private object mlockHisQueryObj = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public bool IsLogin { get { return LoginId > 0; } }

        /// <summary>
        /// 
        /// </summary>
        public long LoginId { get; private set; }

        public TagValueChangedCallBackDelegate TagValueChangedCallBack { get; set; }

        private string mUser;
        private string mPass;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="datas"></param>
        protected override void ProcessData(byte fun, IByteBuffer datas)
        {
            if(fun == ApiFunConst.RealDataPushFun)
            {
                TagValueChangedCallBack?.Invoke(ProcessPushSingleBufferData(datas));
            }
            else
            {
                datas.Retain();
                switch (fun)
                {
                    case ApiFunConst.TagInfoRequest:
                        mInfoRequreData = datas;
                        infoRequreEvent.Set();
                        break;
                    case ApiFunConst.RealDataRequestFun:
                        mRealRequreData = datas;
                        this.realRequreEvent.Set();
                        break;
                    case ApiFunConst.HisDataRequestFun:
                        mHisRequreData = datas;
                        hisRequreEvent.Set();
                        break;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        public bool Login(string username,string password,int timeount= 30000)
        {

            lock (mReceivedDatas)
            {
                if (IsLogin) return true;

                mUser = username;
                mPass = password;
                int size = username.Length + password.Length + 9;
                var mb = GetBuffer(ApiFunConst.TagInfoRequest, size);
                mb.WriteByte(ApiFunConst.Login);
                mb.WriteString(username);
                mb.WriteString(password);
                Send(mb);
                if (infoRequreEvent.WaitOne(timeount))
                {
                    if (mInfoRequreData.ReferenceCount > 0)
                    {
                        LoginId = mInfoRequreData.ReadLong();
                        return IsLogin;
                    }
                }
                //mInfoRequreData?.Release();
                LoginId = -1;
                return IsLogin;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagNames"></param>
        /// <returns></returns>
        public  Dictionary<string,int> GetTagIds( List<string> tagNames, int timeout = 5000)
        {
            Dictionary<string, int> re = new Dictionary<string, int>();

            var mb = GetBuffer(ApiFunConst.TagInfoRequest,tagNames.Count*24+1+9);
            mb.WriteByte(ApiFunConst.GetTagIdByNameFun);
            mb.WriteLong(LoginId);
            mb.WriteInt(tagNames.Count);
            foreach(var vv in tagNames)
            {
                mb.WriteString(vv);
            }
            infoRequreEvent.Reset();
            Send(mb);

            if (infoRequreEvent.WaitOne(timeout))
            {
                for(int i=0;i<tagNames.Count;i++)
                {
                    if(mInfoRequreData.ReadableBytes>0)
                    re.Add(tagNames[i], mInfoRequreData.ReadInt());
                }
            }
            mInfoRequreData?.Release();

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="loginid"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public string GetRunnerDatabase(int timeout = 5000)
        {
            var mb = GetBuffer(ApiFunConst.TagInfoRequest,8+ 1);
            mb.WriteByte(ApiFunConst.GetRunnerDatabase);
            mb.WriteLong(LoginId);
            infoRequreEvent.Reset();
            Send(mb);
            try
            {
                if (infoRequreEvent.WaitOne(timeout))
                {
                    return mInfoRequreData.ReadString();
                }
            }
            finally
            {
                //mInfoRequreData?.ReleaseBuffer();
            }
            return string.Empty;
        }

        #region RealData

        /// <summary>
        /// 
        /// </summary>
        /// <param name="loginid"></param>
        /// <param name="minid"></param>
        /// <param name="maxid"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool RegistorTagValueCallBack(int minid, int maxid, int timeout = 5000)
        {
            lock (mlockRealObj)
            {
                CheckLogin();
                if (!IsLogin) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
                mb.WriteByte(ApiFunConst.RegistorValueCallback);
                mb.WriteLong(this.LoginId);
                mb.WriteInt(minid);
                mb.WriteInt(maxid);
                this.realRequreEvent.Reset();
                Send(mb);
                try
                {
                    if (realRequreEvent.WaitOne(timeout))
                    {
                        return mRealRequreData.ReadByte() > 0;
                    }
                }
                finally
                {
                    //mRealRequreData?.ReleaseBuffer();
                }
                return true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool ClearRegistorTagValueCallBack(int timeout = 5000)
        {
            lock (mlockRealObj)
            {
                CheckLogin();
                if (!IsLogin) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
                mb.WriteByte(ApiFunConst.ResetValueChangeNotify);
                mb.WriteLong(this.LoginId);
                realRequreEvent.Reset();
                Send(mb);

                try
                {
                    if (realRequreEvent.WaitOne(timeout))
                    {
                        return mRealRequreData.ReadByte() > 0;
                    }
                }
                finally
                {
                    //mRealRequreData?.ReleaseBuffer();
                }
                return true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private IByteBuffer GetRealDataInner(List<int> ids,int timeout=5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 +ids.Count* 4);
            mb.WriteByte(ApiFunConst.RequestRealData);
            mb.WriteLong(this.LoginId);
            mb.WriteInt(ids.Count);
            for(int i=0;i<ids.Count;i++)
            {
                mb.WriteInt(ids[i]);
            }
            realRequreEvent.Reset();
            Send(mb);

            try
            {
                if (realRequreEvent.WaitOne(timeout))
                {
                    return mRealRequreData;
                }
            }
            finally
            {
                //mRealRequreData?.ReleaseBuffer();
            }
            

            return null;
        }


        private IByteBuffer GetRealDataInnerValueOnly(List<int> ids, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4);
            mb.WriteByte(ApiFunConst.RequestRealDataValue);
            mb.WriteLong(this.LoginId);
            mb.WriteInt(ids.Count);
            for (int i = 0; i < ids.Count; i++)
            {
                mb.WriteInt(ids[i]);
            }
            realRequreEvent.Reset();
            Send(mb);

            try
            {
                if (realRequreEvent.WaitOne(timeout))
                {
                    return mRealRequreData;
                }
            }
            finally
            {
                //mRealRequreData?.ReleaseBuffer();
            }


            return null;
        }

        private IByteBuffer GetRealDataInnerValueAndQualityOnly(List<int> ids, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4);
            mb.WriteByte(ApiFunConst.RequestRealDataValueAndQuality);
            mb.WriteLong(this.LoginId);
            mb.WriteInt(ids.Count);
            for (int i = 0; i < ids.Count; i++)
            {
                mb.WriteInt(ids[i]);
            }
            realRequreEvent.Reset();
            Send(mb);

            try
            {
                if (realRequreEvent.WaitOne(timeout))
                {
                    return mRealRequreData;
                }
            }
            finally
            {
                //mRealRequreData?.ReleaseBuffer();
            }


            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="ide"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private IByteBuffer GetRealDataInner(int ids, int ide, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
            mb.WriteByte(ApiFunConst.RequestRealData2);
            mb.WriteLong(this.LoginId);
            mb.WriteInt(ids);
            mb.WriteInt(ide);
            realRequreEvent.Reset();
            Send(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="ide"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private IByteBuffer GetRealDataInnerValueOnly(int ids, int ide, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
            mb.WriteByte(ApiFunConst.RequestReal2DataValue);
            mb.WriteLong(this.LoginId);
            mb.WriteInt(ids);
            mb.WriteInt(ide);
            realRequreEvent.Reset();
            Send(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="ide"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private IByteBuffer GetRealDataInnerValueAndQualityOnly(int ids, int ide, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
            mb.WriteByte(ApiFunConst.RequestReal2DataValueAndQuality);
            mb.WriteLong(this.LoginId);
            mb.WriteInt(ids);
            mb.WriteInt(ide);
            realRequreEvent.Reset();
            Send(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData;
            }
            return null;
        }

        protected string ReadString(IByteBuffer buffer)
        {
            return buffer.ReadString(buffer.ReadInt(), Encoding.Unicode);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private Dictionary<int,Tuple<object,DateTime,byte>> ProcessSingleBufferData(IByteBuffer block)
        {
            if (block == null) return null;
            Dictionary<int, Tuple<object, DateTime, byte>> re = new Dictionary<int, Tuple<object, DateTime, byte>>();

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
                        value = (ushort)block.ReadShort();
                        break;
                    case (byte)TagType.Int:
                        value = block.ReadInt();
                        break;
                    case (byte)TagType.UInt:
                        value = (uint)block.ReadInt();
                        break;
                    case (byte)TagType.Long:
                        value = block.ReadLong();
                        break;
                    case (byte)TagType.ULong:
                        value = (ulong)block.ReadLong();
                        break;
                    case (byte)TagType.Float:
                        value = block.ReadFloat();
                        break;
                    case (byte)TagType.Double:
                        value = block.ReadDouble();
                        break;
                    case (byte)TagType.String:
                        value = ReadString(block);
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
                var time = new DateTime(block.ReadLong());
                var qua = block.ReadByte();
                re.Add(vid, new Tuple<object, DateTime, byte>(value,time, qua));
            }
            block.Release();
            return re;
        }

        private Dictionary<int, Tuple<object,byte>> ProcessPushSingleBufferData(IByteBuffer block)
        {
            if (block == null) return null;
            Dictionary<int, Tuple<object,  byte>> re = new Dictionary<int, Tuple<object,  byte>>();

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
                        value = (ushort)block.ReadShort();
                        break;
                    case (byte)TagType.Int:
                        value = block.ReadInt();
                        break;
                    case (byte)TagType.UInt:
                        value = (uint)block.ReadInt();
                        break;
                    case (byte)TagType.Long:
                        value = block.ReadLong();
                        break;
                    case (byte)TagType.ULong:
                        value = (ulong)block.ReadLong();
                        break;
                    case (byte)TagType.Float:
                        value = block.ReadFloat();
                        break;
                    case (byte)TagType.Double:
                        value = block.ReadDouble();
                        break;
                    case (byte)TagType.String:
                        value = ReadString(block);
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
                var qua = block.ReadByte();
                re.Add(vid, new Tuple<object,byte>(value,qua));
            }
            //block.Release();
            return re;
        }


        private Dictionary<int, Tuple<object, byte>> ProcessSingleBufferDataValueAndQuality(IByteBuffer block)
        {
            if (block == null) return null;
            Dictionary<int, Tuple<object,  byte>> re = new Dictionary<int, Tuple<object,  byte>>();

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
                        value = (ushort)block.ReadShort();
                        break;
                    case (byte)TagType.Int:
                        value = block.ReadInt();
                        break;
                    case (byte)TagType.UInt:
                        value = (uint)block.ReadInt();
                        break;
                    case (byte)TagType.Long:
                        value = block.ReadLong();
                        break;
                    case (byte)TagType.ULong:
                        value = (ulong)block.ReadLong();
                        break;
                    case (byte)TagType.Float:
                        value = block.ReadFloat();
                        break;
                    case (byte)TagType.Double:
                        value = block.ReadDouble();
                        break;
                    case (byte)TagType.String:
                        value = ReadString(block);
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
                var qua = block.ReadByte();
                re.Add(vid, new Tuple<object, byte>(value, qua));
            }
            block.Release();
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        private Dictionary<int, object> ProcessSingleBufferDataValue(IByteBuffer block)
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
                    break;
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
                        value = (ushort)block.ReadShort();
                        break;
                    case (byte)TagType.Int:
                        value = block.ReadInt();
                        break;
                    case (byte)TagType.UInt:
                        value = (uint)block.ReadInt();
                        break;
                    case (byte)TagType.Long:
                        value = block.ReadLong();
                        break;
                    case (byte)TagType.ULong:
                        value = (ulong)block.ReadLong();
                        break;
                    case (byte)TagType.Float:
                        value = block.ReadFloat();
                        break;
                    case (byte)TagType.Double:
                        value = block.ReadDouble();
                        break;
                    case (byte)TagType.String:
                        value = ReadString(block);
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
            block.Release();
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<object, DateTime, byte>> GetRealData(List<int> ids, int timeout = 5000)
        {
            lock(mlockRealObj)
            return ProcessSingleBufferData(GetRealDataInner(ids, timeout));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int, object> GetRealDataValueOnly(List<int> ids, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferDataValue(GetRealDataInnerValueOnly(ids, timeout));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<object,byte>> GetRealDataValueAndQualityOnly(List<int> ids, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferDataValueAndQuality(GetRealDataInnerValueAndQualityOnly(ids, timeout));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startId"></param>
        /// <param name="endId"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<object, DateTime, byte>> GetRealData(int startId,int endId, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferData(GetRealDataInner(startId,endId, timeout));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startId"></param>
        /// <param name="endId"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int, object> GetRealDataValueOnly(int startId, int endId, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferDataValue(GetRealDataInnerValueOnly(startId,endId, timeout));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startId"></param>
        /// <param name="endId"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>

        public Dictionary<int, Tuple<object, byte>> GetRealDataValueAndQualityOnly(int startId, int endId, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferDataValueAndQuality(GetRealDataInnerValueAndQualityOnly(startId, endId, timeout));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="valueType"></param>
        /// <param name="value"></param>
        public bool SetTagValue(int id,byte valueType,object value,int timeout=5000)
        {
            lock (mlockRealSetObj)
            {
                CheckLogin();
                if (!IsLogin) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 30);
                mb.WriteByte(ApiFunConst.SetDataValue);
                mb.WriteLong(this.LoginId);
                mb.WriteInt(1);
                mb.WriteInt(id);
                mb.WriteByte(valueType);
                switch (valueType)
                {
                    case (byte)TagType.Bool:
                        mb.WriteByte((byte)value);
                        break;
                    case (byte)TagType.Byte:
                        mb.WriteByte((byte)value);
                        break;
                    case (byte)TagType.Short:
                        mb.WriteShort((short)value);
                        break;
                    case (byte)TagType.UShort:
                        mb.WriteUnsignedShort((ushort)value);
                        break;
                    case (byte)TagType.Int:
                        mb.WriteInt((int)value);
                        break;
                    case (byte)TagType.UInt:
                        mb.WriteInt((int)value);
                        break;
                    case (byte)TagType.Long:
                    case (byte)TagType.ULong:
                        mb.WriteLong((long)value);
                        break;
                    case (byte)TagType.Float:
                        mb.WriteFloat((float)value);
                        break;
                    case (byte)TagType.Double:
                        mb.WriteDouble((double)value);
                        break;
                    case (byte)TagType.String:
                        string sval = value.ToString();
                        mb.WriteInt(sval.Length);
                        mb.WriteString(sval, Encoding.Unicode);
                        break;
                    case (byte)TagType.DateTime:
                        mb.WriteLong(((DateTime)value).Ticks);
                        break;
                    case (byte)TagType.IntPoint:
                        mb.WriteInt(((IntPointData)value).X);
                        mb.WriteInt(((IntPointData)value).Y);
                        break;
                    case (byte)TagType.UIntPoint:
                        mb.WriteInt((int)((UIntPointData)value).X);
                        mb.WriteInt((int)((UIntPointData)value).Y);
                        break;
                    case (byte)TagType.IntPoint3:
                        mb.WriteInt(((IntPoint3Data)value).X);
                        mb.WriteInt(((IntPoint3Data)value).Y);
                        mb.WriteInt(((IntPoint3Data)value).Z);
                        break;
                    case (byte)TagType.UIntPoint3:
                        mb.WriteInt((int)((UIntPoint3Data)value).X);
                        mb.WriteInt((int)((UIntPoint3Data)value).Y);
                        mb.WriteInt((int)((UIntPoint3Data)value).Z);
                        break;
                    case (byte)TagType.LongPoint:
                        mb.WriteLong(((LongPointData)value).X);
                        mb.WriteLong(((LongPointData)value).Y);
                        break;
                    case (byte)TagType.ULongPoint:
                        mb.WriteLong((long)((ULongPointData)value).X);
                        mb.WriteLong((long)((ULongPointData)value).Y);
                        break;
                    case (byte)TagType.LongPoint3:
                        mb.WriteLong(((LongPoint3Data)value).X);
                        mb.WriteLong(((LongPoint3Data)value).Y);
                        mb.WriteLong(((LongPoint3Data)value).Z);
                        break;
                    case (byte)TagType.ULongPoint3:
                        mb.WriteLong((long)((ULongPoint3Data)value).X);
                        mb.WriteLong((long)((ULongPoint3Data)value).Y);
                        mb.WriteLong((long)((ULongPoint3Data)value).Z);
                        break;
                }
                realRequreEvent.Reset();
                Send(mb);
                try
                {
                    if (realRequreEvent.WaitOne(timeout))
                    {
                        return mRealRequreData.ReadByte() > 0;
                    }
                }
                finally
                {
                    //mRealRequreData?.ReleaseBuffer();
                }
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="valueType"></param>
        /// <param name="value"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValue(List<int> id, List<byte> valueType, List<object> value, int timeout = 5000)
        {
            lock (mlockRealSetObj)
            {
                CheckLogin();
                if (!IsLogin) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 30);
                mb.WriteByte(ApiFunConst.SetDataValue);
                mb.WriteLong(this.LoginId);
                mb.WriteInt(1);
                for (int i = 0; i < id.Count; i++)
                {
                    mb.WriteInt(id[i]);
                    switch (valueType[i])
                    {
                        case (byte)TagType.Bool:
                            mb.WriteByte((byte)value[i]);
                            break;
                        case (byte)TagType.Byte:
                            mb.WriteByte((byte)value[i]);
                            break;
                        case (byte)TagType.Short:
                            mb.WriteShort((short)value[i]);
                            break;
                        case (byte)TagType.UShort:
                            mb.WriteUnsignedShort((ushort)value[i]);
                            break;
                        case (byte)TagType.Int:
                            mb.WriteInt((int)value[i]);
                            break;
                        case (byte)TagType.UInt:
                            mb.WriteInt((int)value[i]);
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            mb.WriteLong((long)value[i]);
                            break;
                        case (byte)TagType.Float:
                            mb.WriteFloat((float)value[i]);
                            break;
                        case (byte)TagType.Double:
                            mb.WriteDouble((double)value[i]);
                            break;
                        case (byte)TagType.String:
                            string sval = value[i].ToString();
                            mb.WriteInt(sval.Length);
                            mb.WriteString(sval, Encoding.Unicode);
                            break;
                        case (byte)TagType.DateTime:
                            mb.WriteLong(((DateTime)value[i]).Ticks);
                            break;
                        case (byte)TagType.IntPoint:
                            mb.WriteInt(((IntPointData)value[i]).X);
                            mb.WriteInt(((IntPointData)value[i]).Y);
                            break;
                        case (byte)TagType.UIntPoint:
                            mb.WriteInt((int)((UIntPointData)value[i]).X);
                            mb.WriteInt((int)((UIntPointData)value[i]).Y);
                            break;
                        case (byte)TagType.IntPoint3:
                            mb.WriteInt(((IntPoint3Data)value[i]).X);
                            mb.WriteInt(((IntPoint3Data)value[i]).Y);
                            mb.WriteInt(((IntPoint3Data)value[i]).Z);
                            break;
                        case (byte)TagType.UIntPoint3:
                            mb.WriteInt((int)((UIntPoint3Data)value[i]).X);
                            mb.WriteInt((int)((UIntPoint3Data)value[i]).Y);
                            mb.WriteInt((int)((UIntPoint3Data)value[i]).Z);
                            break;
                        case (byte)TagType.LongPoint:
                            mb.WriteLong(((LongPointData)value[i]).X);
                            mb.WriteLong(((LongPointData)value[i]).Y);
                            break;
                        case (byte)TagType.ULongPoint:
                            mb.WriteLong((long)((ULongPointData)value[i]).X);
                            mb.WriteLong((long)((ULongPointData)value[i]).Y);
                            break;
                        case (byte)TagType.LongPoint3:
                            mb.WriteLong(((LongPoint3Data)value[i]).X);
                            mb.WriteLong(((LongPoint3Data)value[i]).Y);
                            mb.WriteLong(((LongPoint3Data)value[i]).Z);
                            break;
                        case (byte)TagType.ULongPoint3:
                            mb.WriteLong((long)((ULongPoint3Data)value[i]).X);
                            mb.WriteLong((long)((ULongPoint3Data)value[i]).Y);
                            mb.WriteLong((long)((ULongPoint3Data)value[i]).Z);
                            break;
                    }
                }
                realRequreEvent.Reset();
                Send(mb);
                try
                {
                    if (realRequreEvent.WaitOne(timeout))
                    {
                        return mRealRequreData.ReadByte() > 0;
                    }
                }
                finally
                {
                    //mRealRequreData?.ReleaseBuffer();
                }
                return false;
            }
        }


        #endregion

        private void CheckLogin()
        {
            if(LoginId<=0)
            {
                Login(mUser, mPass);
            }
        }

        #region HisData

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public IByteBuffer QueryAllHisValue(int id,DateTime startTime,DateTime endTime,int timeout=5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                mb.WriteByte(ApiFunConst.RequestAllHisData);
                mb.WriteLong(this.LoginId);
                mb.WriteInt(id);
                mb.WriteLong(startTime.Ticks);
                mb.WriteLong(endTime.Ticks);

                this.hisRequreEvent.Reset();
                Send(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableBytes > 1)
                    {
                        return mHisRequreData;
                    }
                }
                finally
                {
                    //mHisRequreData?.ReleaseBuffer();
                }
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="matchType"></param>
        /// <returns></returns>
        public IByteBuffer QueryHisValueAtTimes(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + times.Count * 8 + 5);
                mb.WriteByte(ApiFunConst.RequestHisDatasByTimePoint);
                mb.WriteLong(this.LoginId);
                mb.WriteInt(id);
                mb.WriteByte((byte)matchType);
                mb.WriteInt(times.Count);
                for (int i = 0; i < times.Count; i++)
                {
                    mb.WriteLong(times[i].Ticks);
                }

                this.hisRequreEvent.Reset();
                Send(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableBytes > 1)
                    {
                        return mHisRequreData;
                    }
                }
                finally
                {
                    //mHisRequreData?.ReleaseBuffer();
                }
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="span"></param>
        /// <param name="matchType"></param>
        /// <returns></returns>
        public IByteBuffer QueryHisValueForTimeSpan(int id,DateTime startTime,DateTime endTime,TimeSpan span,QueryValueMatchType matchType, int timeout = 5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 24 + 5);
                mb.WriteByte(ApiFunConst.RequestHisDataByTimeSpan);
                mb.WriteLong(this.LoginId);
                mb.WriteInt(id);
                mb.WriteByte((byte)matchType);
                mb.WriteLong(startTime.Ticks);
                mb.WriteLong(endTime.Ticks);
                mb.WriteLong(span.Ticks);
                this.hisRequreEvent.Reset();
                Send(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableBytes > 1)
                    {
                        return mHisRequreData;
                    }
                }
                finally
                {
                    //mHisRequreData?.ReleaseBuffer();
                }
                return null;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult QueryStatisticsValue(int id, DateTime startTime, DateTime endTime, int timeout = 5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                mb.WriteByte(ApiFunConst.RequestStatisticData);
                mb.WriteLong(this.LoginId);
                mb.WriteInt(id);
                mb.WriteLong(startTime.Ticks);
                mb.WriteLong(endTime.Ticks);

                this.hisRequreEvent.Reset();
                Send(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableBytes > 1)
                    {
                        TagType tp = (TagType)mHisRequreData.ReadByte();
                        return ProcessStatisticsResult(mHisRequreData,tp);
                    }
                }
                finally
                {
                    //mHisRequreData?.ReleaseBuffer();
                }
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="matchType"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult QueryHisValueAtTimes(int id, List<DateTime> times, int timeout = 5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + times.Count * 8 + 5);
                mb.WriteByte(ApiFunConst.RequestHisDatasByTimePoint);
                mb.WriteLong(this.LoginId);
                mb.WriteInt(id);
                mb.WriteInt(times.Count);
                for (int i = 0; i < times.Count; i++)
                {
                    mb.WriteLong(times[i].Ticks);
                }

                this.hisRequreEvent.Reset();
                Send(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableBytes > 1)
                    {
                        TagType tp = (TagType)mHisRequreData.ReadByte();
                        return ProcessStatisticsResult(mHisRequreData, tp);
                    }
                }
                finally
                {
                    //mHisRequreData?.ReleaseBuffer();
                }
                return null;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data"></param>
        /// <param name="tp"></param>
        /// <returns></returns>
        private unsafe NumberStatisticsQueryResult ProcessStatisticsResult(IByteBuffer data, TagType tp)
        {
            int count = data.ReadInt();
            NumberStatisticsQueryResult re = new NumberStatisticsQueryResult(count);
            Marshal.Copy(data.Array, data.ArrayOffset + data.ReaderIndex, re.MemoryHandle, data.ReadableBytes);
            re.Count = count;
            return re;
        }

        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
