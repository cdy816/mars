using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Cdy.Tag;
using Cheetah;

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

    public class ApiClient:SocketClient2
    {

        #region ... Variables  ...
        
        private ManualResetEvent infoRequreEvent = new ManualResetEvent(false);

        private ByteBuffer mInfoRequreData;

        private ManualResetEvent hisRequreEvent = new ManualResetEvent(false);

        private ByteBuffer mHisRequreData;

        private ManualResetEvent realRequreEvent = new ManualResetEvent(false);

        private ByteBuffer mRealRequreData;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<byte, ByteBuffer> mReceivedDatas = new Dictionary<byte, ByteBuffer>();

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
        protected override void ProcessData(byte fun, ByteBuffer datas)
        {
            if(fun == ApiFunConst.RealDataPushFun)
            {
                TagValueChangedCallBack?.Invoke(ProcessPushSingleBufferData(datas));
            }
            else
            {
                datas.IncRef();
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
                mb.Write(ApiFunConst.Login);
                mb.Write(username);
                mb.Write(password);
                SendData(mb);
                if (infoRequreEvent.WaitOne(timeount))
                {
                    if (mInfoRequreData.RefCount > 0)
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
            mb.Write(ApiFunConst.GetTagIdByNameFun);
            mb.Write(LoginId);
            mb.Write(tagNames.Count);
            foreach(var vv in tagNames)
            {
                mb.Write(vv);
            }
            infoRequreEvent.Reset();
            SendData(mb);

            if (infoRequreEvent.WaitOne(timeout))
            {
                for(int i=0;i<tagNames.Count;i++)
                {
                    if(mInfoRequreData.ReadableCount>0)
                    re.Add(tagNames[i], mInfoRequreData.ReadInt());
                }
            }
            mInfoRequreData?.UnlockAndReturn();

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
            mb.Write(ApiFunConst.GetRunnerDatabase);
            mb.Write(LoginId);
            infoRequreEvent.Reset();
            SendData(mb);
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
                mb.Write(ApiFunConst.RegistorValueCallback);
                mb.Write(this.LoginId);
                mb.Write(minid);
                mb.Write(maxid);
                this.realRequreEvent.Reset();
                SendData(mb);
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
                mb.Write(ApiFunConst.ResetValueChangeNotify);
                mb.Write(this.LoginId);
                realRequreEvent.Reset();
                SendData(mb);

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
        private ByteBuffer GetRealDataInner(List<int> ids,int timeout=5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 +ids.Count* 4);
            mb.Write(ApiFunConst.RequestRealData);
            mb.Write(this.LoginId);
            mb.Write(ids.Count);
            for(int i=0;i<ids.Count;i++)
            {
                mb.Write(ids[i]);
            }
            realRequreEvent.Reset();
            SendData(mb);

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


        private ByteBuffer GetRealDataInnerValueOnly(List<int> ids, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4);
            mb.Write(ApiFunConst.RequestRealDataValue);
            mb.Write(this.LoginId);
            mb.Write(ids.Count);
            for (int i = 0; i < ids.Count; i++)
            {
                mb.Write(ids[i]);
            }
            realRequreEvent.Reset();
            SendData(mb);

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

        private ByteBuffer GetRealDataInnerValueAndQualityOnly(List<int> ids, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4);
            mb.Write(ApiFunConst.RequestRealDataValueAndQuality);
            mb.Write(this.LoginId);
            mb.Write(ids.Count);
            for (int i = 0; i < ids.Count; i++)
            {
                mb.Write(ids[i]);
            }
            realRequreEvent.Reset();
            SendData(mb);

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
        private ByteBuffer GetRealDataInner(int ids, int ide, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
            mb.Write(ApiFunConst.RequestRealData2);
            mb.Write(this.LoginId);
            mb.Write(ids);
            mb.Write(ide);
            realRequreEvent.Reset();
            SendData(mb);

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
        private ByteBuffer GetRealDataInnerValueOnly(int ids, int ide, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
            mb.Write(ApiFunConst.RequestReal2DataValue);
            mb.Write(this.LoginId);
            mb.Write(ids);
            mb.Write(ide);
            realRequreEvent.Reset();
            SendData(mb);

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
        private ByteBuffer GetRealDataInnerValueAndQualityOnly(int ids, int ide, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
            mb.Write(ApiFunConst.RequestReal2DataValueAndQuality);
            mb.Write(this.LoginId);
            mb.Write(ids);
            mb.Write(ide);
            realRequreEvent.Reset();
            SendData(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData;
            }
            return null;
        }

        protected string ReadString(ByteBuffer buffer)
        {
            return buffer.ReadString(Encoding.Unicode);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private Dictionary<int,Tuple<object,DateTime,byte>> ProcessSingleBufferData(ByteBuffer block)
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
            block.UnlockAndReturn();
            return re;
        }

        private Dictionary<int, Tuple<object,byte>> ProcessPushSingleBufferData(ByteBuffer block)
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
            block.UnlockAndReturn();
            return re;
        }


        private Dictionary<int, Tuple<object, byte>> ProcessSingleBufferDataValueAndQuality(ByteBuffer block)
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
            block.UnlockAndReturn();
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        private Dictionary<int, object> ProcessSingleBufferDataValue(ByteBuffer block)
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
            block.UnlockAndReturn();
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
                mb.Write(ApiFunConst.SetDataValue);
                mb.Write(this.LoginId);
                mb.Write(1);
                mb.Write(id);
                mb.Write(valueType);
                switch (valueType)
                {
                    case (byte)TagType.Bool:
                        mb.Write((byte)value);
                        break;
                    case (byte)TagType.Byte:
                        mb.Write((byte)value);
                        break;
                    case (byte)TagType.Short:
                        mb.Write((short)value);
                        break;
                    case (byte)TagType.UShort:
                        mb.Write((ushort)value);
                        break;
                    case (byte)TagType.Int:
                        mb.Write((int)value);
                        break;
                    case (byte)TagType.UInt:
                        mb.Write((int)value);
                        break;
                    case (byte)TagType.Long:
                    case (byte)TagType.ULong:
                        mb.Write((long)value);
                        break;
                    case (byte)TagType.Float:
                        mb.Write((float)value);
                        break;
                    case (byte)TagType.Double:
                        mb.Write((double)value);
                        break;
                    case (byte)TagType.String:
                        string sval = value.ToString();
                        mb.Write(sval, Encoding.Unicode);
                        break;
                    case (byte)TagType.DateTime:
                        mb.Write(((DateTime)value).Ticks);
                        break;
                    case (byte)TagType.IntPoint:
                        mb.Write(((IntPointData)value).X);
                        mb.Write(((IntPointData)value).Y);
                        break;
                    case (byte)TagType.UIntPoint:
                        mb.Write((int)((UIntPointData)value).X);
                        mb.Write((int)((UIntPointData)value).Y);
                        break;
                    case (byte)TagType.IntPoint3:
                        mb.Write(((IntPoint3Data)value).X);
                        mb.Write(((IntPoint3Data)value).Y);
                        mb.Write(((IntPoint3Data)value).Z);
                        break;
                    case (byte)TagType.UIntPoint3:
                        mb.Write((int)((UIntPoint3Data)value).X);
                        mb.Write((int)((UIntPoint3Data)value).Y);
                        mb.Write((int)((UIntPoint3Data)value).Z);
                        break;
                    case (byte)TagType.LongPoint:
                        mb.Write(((LongPointData)value).X);
                        mb.Write(((LongPointData)value).Y);
                        break;
                    case (byte)TagType.ULongPoint:
                        mb.Write((long)((ULongPointData)value).X);
                        mb.Write((long)((ULongPointData)value).Y);
                        break;
                    case (byte)TagType.LongPoint3:
                        mb.Write(((LongPoint3Data)value).X);
                        mb.Write(((LongPoint3Data)value).Y);
                        mb.Write(((LongPoint3Data)value).Z);
                        break;
                    case (byte)TagType.ULongPoint3:
                        mb.Write((long)((ULongPoint3Data)value).X);
                        mb.Write((long)((ULongPoint3Data)value).Y);
                        mb.Write((long)((ULongPoint3Data)value).Z);
                        break;
                }
                realRequreEvent.Reset();
                SendData(mb);
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
                mb.Write(ApiFunConst.SetDataValue);
                mb.Write(this.LoginId);
                mb.Write(1);
                for (int i = 0; i < id.Count; i++)
                {
                    mb.Write(id[i]);
                    switch (valueType[i])
                    {
                        case (byte)TagType.Bool:
                            mb.Write((byte)value[i]);
                            break;
                        case (byte)TagType.Byte:
                            mb.Write((byte)value[i]);
                            break;
                        case (byte)TagType.Short:
                            mb.Write((short)value[i]);
                            break;
                        case (byte)TagType.UShort:
                            mb.Write((ushort)value[i]);
                            break;
                        case (byte)TagType.Int:
                            mb.Write((int)value[i]);
                            break;
                        case (byte)TagType.UInt:
                            mb.Write((int)value[i]);
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            mb.Write((long)value[i]);
                            break;
                        case (byte)TagType.Float:
                            mb.Write((float)value[i]);
                            break;
                        case (byte)TagType.Double:
                            mb.Write((double)value[i]);
                            break;
                        case (byte)TagType.String:
                            string sval = value[i].ToString();
                          //  mb.Write(sval.Length);
                            mb.Write(sval, Encoding.Unicode);
                            break;
                        case (byte)TagType.DateTime:
                            mb.Write(((DateTime)value[i]).Ticks);
                            break;
                        case (byte)TagType.IntPoint:
                            mb.Write(((IntPointData)value[i]).X);
                            mb.Write(((IntPointData)value[i]).Y);
                            break;
                        case (byte)TagType.UIntPoint:
                            mb.Write((int)((UIntPointData)value[i]).X);
                            mb.Write((int)((UIntPointData)value[i]).Y);
                            break;
                        case (byte)TagType.IntPoint3:
                            mb.Write(((IntPoint3Data)value[i]).X);
                            mb.Write(((IntPoint3Data)value[i]).Y);
                            mb.Write(((IntPoint3Data)value[i]).Z);
                            break;
                        case (byte)TagType.UIntPoint3:
                            mb.Write((int)((UIntPoint3Data)value[i]).X);
                            mb.Write((int)((UIntPoint3Data)value[i]).Y);
                            mb.Write((int)((UIntPoint3Data)value[i]).Z);
                            break;
                        case (byte)TagType.LongPoint:
                            mb.Write(((LongPointData)value[i]).X);
                            mb.Write(((LongPointData)value[i]).Y);
                            break;
                        case (byte)TagType.ULongPoint:
                            mb.Write((long)((ULongPointData)value[i]).X);
                            mb.Write((long)((ULongPointData)value[i]).Y);
                            break;
                        case (byte)TagType.LongPoint3:
                            mb.Write(((LongPoint3Data)value[i]).X);
                            mb.Write(((LongPoint3Data)value[i]).Y);
                            mb.Write(((LongPoint3Data)value[i]).Z);
                            break;
                        case (byte)TagType.ULongPoint3:
                            mb.Write((long)((ULongPoint3Data)value[i]).X);
                            mb.Write((long)((ULongPoint3Data)value[i]).Y);
                            mb.Write((long)((ULongPoint3Data)value[i]).Z);
                            break;
                    }
                }
                realRequreEvent.Reset();
                SendData(mb);
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
        public ByteBuffer QueryAllHisValue(int id,DateTime startTime,DateTime endTime,int timeout=5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                mb.Write(ApiFunConst.RequestAllHisData);
                mb.Write(this.LoginId);
                mb.Write(id);
                mb.Write(startTime.Ticks);
                mb.Write(endTime.Ticks);

                this.hisRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
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
        public ByteBuffer QueryHisValueAtTimes(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + times.Count * 8 + 5);
                mb.Write(ApiFunConst.RequestHisDatasByTimePoint);
                mb.Write(this.LoginId);
                mb.Write(id);
                mb.Write((byte)matchType);
                mb.Write(times.Count);
                for (int i = 0; i < times.Count; i++)
                {
                    mb.Write(times[i].Ticks);
                }

                this.hisRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
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
        public ByteBuffer QueryHisValueForTimeSpan(int id,DateTime startTime,DateTime endTime,TimeSpan span,QueryValueMatchType matchType, int timeout = 5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 24 + 5);
                mb.Write(ApiFunConst.RequestHisDataByTimeSpan);
                mb.Write(this.LoginId);
                mb.Write(id);
                mb.Write((byte)matchType);
                mb.Write(startTime.Ticks);
                mb.Write(endTime.Ticks);
                mb.Write(span.Ticks);
                this.hisRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
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
                mb.Write(ApiFunConst.RequestStatisticData);
                mb.Write(this.LoginId);
                mb.Write(id);
                mb.Write(startTime.Ticks);
                mb.Write(endTime.Ticks);

                this.hisRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
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
                mb.Write(ApiFunConst.RequestHisDatasByTimePoint);
                mb.Write(this.LoginId);
                mb.Write(id);
                mb.Write(times.Count);
                for (int i = 0; i < times.Count; i++)
                {
                    mb.Write(times[i].Ticks);
                }

                this.hisRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
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
        private unsafe NumberStatisticsQueryResult ProcessStatisticsResult(ByteBuffer data, TagType tp)
        {
            int count = data.ReadInt();
            NumberStatisticsQueryResult re = new NumberStatisticsQueryResult(count);
            data.CopyTo(re.MemoryHandle, data.ReadIndex, 0, data.ReadableCount);
           // Marshal.Copy(data.Array, data.ArrayOffset + data.ReaderIndex, re.MemoryHandle, data.ReadableBytes);
            re.Count = count;
            return re;
        }

        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
