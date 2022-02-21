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

        public const byte ListAllTagFun = 2;

        public const byte ListTagGroup = 3;

        public const byte GetTagByGroup = 4;

        public const byte Hart = 5;

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
        /// 
        /// </summary>
        public const byte ValueChangeNotify2 = 22;

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

        //
        public const byte RequestFindTagValue = 5;

        //
        public const byte RequestFindTagValues = 6;

        //
        public const byte RequestCalTagValueKeepTime = 7;

        //
        public const byte RequestCalNumberTagAvgValue = 8;

        //
        public const byte RequestFindNumberTagMaxValue = 9;

        //
        public const byte RequestFindNumberTagMinValue = 10;
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

        private object mTagInfoObj = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 是否已经登录
        /// </summary>
        public bool IsLogin { get { return LoginId > 0; } }

        /// <summary>
        /// 登录ID
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
                //datas.IncRef();
                switch (fun)
                {
                    case ApiFunConst.TagInfoRequest:
                        mInfoRequreData?.UnlockAndReturn();
                        mInfoRequreData = datas;
                        infoRequreEvent.Set();
                        break;
                    case ApiFunConst.RealDataRequestFun:
                        mRealRequreData?.UnlockAndReturn();
                        mRealRequreData = datas;
                        this.realRequreEvent.Set();
                        break;
                    case ApiFunConst.HisDataRequestFun:
                        mHisRequreData?.UnlockAndReturn();
                        mHisRequreData = datas;
                        hisRequreEvent.Set();
                        break;
                }
            }
        }

        /// <summary>
        /// 登录
        /// </summary>
        /// <param name="username">用户名</param>
        /// <param name="password">密码</param>
        public bool Login(string username,string password,int timeount= 30000)
        {

            lock (mTagInfoObj)
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
                    try
                    {
                        if (mInfoRequreData != null && (mInfoRequreData.WriteIndex - mInfoRequreData.ReadIndex) > 4)
                        {
                            LoginId = mInfoRequreData.ReadLong();
                            return IsLogin;
                        }
                    }
                    finally
                    {
                        mInfoRequreData?.UnlockAndReturn();
                    }
                }
                //mInfoRequreData?.Release();
                LoginId = -1;
                return IsLogin;
            }
        }

        /// <summary>
        /// 登录
        /// </summary>
        /// <returns></returns>
        public bool Login()
        {
            return Login(mUser, mPass);
        }


        /// <summary>
        /// 心跳
        /// </summary>
        public void Hart()
        {
            lock (mTagInfoObj)
            {
                var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
                mb.Write(ApiFunConst.ListAllTagFun);
                mb.Write(LoginId);
                SendData(mb);
            }
        }

        /// <summary>
        /// 根据变量名称获取变量的ID
        /// </summary>
        /// <param name="tagNames">变量名集合</param>
        /// <param name="timeout">超时</param>
        /// <returns>变量名称、Id</returns>
        public  Dictionary<string,int> GetTagIds( List<string> tagNames, int timeout = 5000)
        {
            Dictionary<string, int> re = new Dictionary<string, int>();

            lock (mTagInfoObj)
            {
                var mb = GetBuffer(ApiFunConst.TagInfoRequest, tagNames.Count * 24 + 1 + 9);
                mb.Write(ApiFunConst.GetTagIdByNameFun);
                mb.Write(LoginId);
                mb.Write(tagNames.Count);
                foreach (var vv in tagNames)
                {
                    mb.Write(vv);
                }
                infoRequreEvent.Reset();
                SendData(mb);

                if (infoRequreEvent.WaitOne(timeout))
                {
                    if (mInfoRequreData != null && (mInfoRequreData.WriteIndex - mInfoRequreData.ReadIndex) > 4)
                    {
                        for (int i = 0; i < tagNames.Count; i++)
                        {
                            re.Add(tagNames[i], mInfoRequreData.ReadInt());
                        }
                    }
                    mInfoRequreData?.UnlockAndReturn();
                }
                return re;
            }
        }

        /// <summary>
        /// 枚举所有变量
        /// </summary>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public Dictionary<string, int> ListAllTag(int timeout = 30000)
        {
            Dictionary<string, int> re = new Dictionary<string, int>();

            lock (mTagInfoObj)
            {
                var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
                mb.Write(ApiFunConst.ListAllTagFun);
                mb.Write(LoginId);
                infoRequreEvent.Reset();
                SendData(mb);

                if (infoRequreEvent.WaitOne(timeout))
                {
                    try
                    {
                        if (mInfoRequreData != null && (mInfoRequreData.WriteIndex - mInfoRequreData.ReadIndex) > 4)
                        {
                            int count = mInfoRequreData.ReadInt();
                            if (count > 0)
                            {
                                var datas = mInfoRequreData.ReadBytes(count);
                                using (System.IO.MemoryStream ms = new System.IO.MemoryStream(datas))
                                {
                                    using (System.IO.Compression.GZipStream gs = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress))
                                    {
                                        using (System.IO.MemoryStream tms = new System.IO.MemoryStream())
                                        {
                                            gs.CopyTo(tms);
                                            var sdata = Encoding.Unicode.GetString(tms.GetBuffer(), 0, (int)tms.Position);
                                            if (sdata.Length > 0)
                                            {
                                                foreach (var vv in sdata.Split(";"))
                                                {
                                                    var vdata = vv.Split(",");
                                                    if(vdata.Length==2)
                                                    re.Add(vdata[1], int.Parse(vdata[0]));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch
                    {

                    }
                    mInfoRequreData?.UnlockAndReturn();
                }
                

                return re;
            }
        }

        /// <summary>
        /// 枚举所有变量组
        /// </summary>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public List<string> ListALlTagGroup(int timeout = 30000)
        {
            List<string> re = new List<string>();

            lock (mTagInfoObj)
            {
                var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
                mb.Write(ApiFunConst.ListTagGroup);
                mb.Write(LoginId);
                infoRequreEvent.Reset();
                SendData(mb);

                if (infoRequreEvent.WaitOne(timeout))
                {
                    try
                    {
                        if (mInfoRequreData != null && (mInfoRequreData.WriteIndex - mInfoRequreData.ReadIndex) > 4)
                        {
                            int count = mInfoRequreData.ReadInt();
                            if (count > 0)
                            {
                                var datas = mInfoRequreData.ReadBytes(count);
                                using (System.IO.MemoryStream ms = new System.IO.MemoryStream(datas))
                                {
                                    using (System.IO.Compression.GZipStream gs = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress))
                                    {
                                        using (System.IO.MemoryStream tms = new System.IO.MemoryStream())
                                        {
                                            gs.CopyTo(tms);
                                            var sdata = Encoding.Unicode.GetString(tms.GetBuffer(),0,(int)tms.Position);
                                            if (sdata.Length > 0)
                                            {
                                                re.AddRange(sdata.Split(","));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch
                    {

                    }
                    mInfoRequreData?.UnlockAndReturn();
                }
                return re;
            }
        }

        /// <summary>
        /// 获取某个变量组下的变量
        /// </summary>
        /// <param name="group">组</param>
        /// <param name="timeout">超时 </param>
        /// <returns></returns>
        public Dictionary<string, int> ListTagByGroup(string group,int timeout = 30000)
        {
            Dictionary<string, int> re = new Dictionary<string, int>();

            lock (mTagInfoObj)
            {
                var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9+256);
                mb.Write(ApiFunConst.GetTagByGroup);
                mb.Write(LoginId);
                mb.Write(group);
                infoRequreEvent.Reset();
                SendData(mb);

                if (infoRequreEvent.WaitOne(timeout))
                {
                    try
                    {
                        if (mInfoRequreData != null && (mInfoRequreData.WriteIndex - mInfoRequreData.ReadIndex) > 4)
                        {
                            int count = mInfoRequreData.ReadInt();
                            if (count > 0)
                            {
                                var datas = mInfoRequreData.ReadBytes(count);
                                using (System.IO.MemoryStream ms = new System.IO.MemoryStream(datas))
                                {
                                    using (System.IO.Compression.GZipStream gs = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress))
                                    {
                                        using (System.IO.MemoryStream tms = new System.IO.MemoryStream())
                                        {
                                            gs.CopyTo(tms);
                                            var sdata = Encoding.Unicode.GetString(tms.GetBuffer(), 0, (int)tms.Position);
                                            if (sdata.Length > 0)
                                            {
                                                foreach (var vv in sdata.Split(";"))
                                                {
                                                    var vdata = vv.Split(",");
                                                    if(vdata.Length==2)
                                                    re.Add(vdata[1], int.Parse(vdata[0]));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch
                    {

                    }
                    mInfoRequreData?.UnlockAndReturn();
                }


                return re;
            }
        }

        /// <summary>
        /// 获取当前运行的数据的名称
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public string GetRunnerDatabase(int timeout = 5000)
        {
            lock (mTagInfoObj)
            {
                var mb = GetBuffer(ApiFunConst.TagInfoRequest, 8 + 1);
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
                    mInfoRequreData?.UnlockAndReturn();
                }
                return string.Empty;
            }
           
        }

        #region RealData

        /// <summary>
        /// 订购变量值改变通知
        /// </summary>
        /// <param name="minid">变量ID最小值</param>
        /// <param name="maxid">变量ID最大值</param>
        /// <param name="timeout">超时</param>
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
                    mRealRequreData?.UnlockAndReturn();
                }
                return true;
            }
        }

        /// <summary>
        /// 订购变量值改变通知
        /// </summary>
        /// <param name="ids">变量ID集合</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public bool RegistorTagValueCallBack(List<int> ids, int timeout = 5000)
        {
            lock (mlockRealObj)
            {
                CheckLogin();
                if (!IsLogin) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 1+4+ids.Count*4);
                mb.Write(ApiFunConst.RegistorValueCallback);
                mb.Write(this.LoginId);
                mb.Write(ids.Count);
                foreach(var vv in ids)
                {
                    mb.Write(vv);
                }
               
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
                    mRealRequreData?.UnlockAndReturn();
                }
                return true;
            }
        }

        /// <summary>
        /// 清除变量值改变通知
        /// </summary>
        /// <param name="timeout">超时</param>
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
                    mRealRequreData?.UnlockAndReturn();
                }
                return true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private ByteBuffer GetRealDataInner(List<int> ids,bool nocach=false,int timeout=5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 +ids.Count* 4+1);
            mb.Write(ApiFunConst.RequestRealData);
            mb.Write(this.LoginId);
            mb.Write(ids.Count);
            for(int i=0;i<ids.Count;i++)
            {
                mb.Write(ids[i]);
            }
            mb.Write(Convert.ToByte(nocach));
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
        /// <param name="nocach"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private ByteBuffer GetRealDataInnerValueOnly(List<int> ids,bool nocach=false, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4+1);
            mb.Write(ApiFunConst.RequestRealDataValue);
            mb.Write(this.LoginId);
            mb.Write(ids.Count);
            for (int i = 0; i < ids.Count; i++)
            {
                mb.Write(ids[i]);
            }
            mb.Write(Convert.ToByte(nocach));
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
        /// <param name="nocach"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private ByteBuffer GetRealDataInnerValueAndQualityOnly(List<int> ids, bool nocach=false,int timeout = 5000)
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
            mb.Write(Convert.ToByte(nocach));
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
        private ByteBuffer GetRealDataInner(int ids, int ide,bool nocach=false, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8+1);
            mb.Write(ApiFunConst.RequestRealData2);
            mb.Write(this.LoginId);
            mb.Write(ids);
            mb.Write(ide);
            mb.Write(Convert.ToByte(nocach));
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
        private ByteBuffer GetRealDataInnerValueOnly(int ids, int ide,bool nocach=false, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
            mb.Write(ApiFunConst.RequestReal2DataValue);
            mb.Write(this.LoginId);
            mb.Write(ids);
            mb.Write(ide);
            mb.Write(Convert.ToByte(nocach));
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
        private ByteBuffer GetRealDataInnerValueAndQualityOnly(int ids, int ide,bool nocach=false, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
            mb.Write(ApiFunConst.RequestReal2DataValueAndQuality);
            mb.Write(this.LoginId);
            mb.Write(ids);
            mb.Write(ide);
            mb.Write(Convert.ToByte(nocach));
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
        /// <param name="buffer"></param>
        /// <returns></returns>
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
            if (block == null || block.Length<1) return null;
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
                var tk = block.ReadLong();
                var time = tk>0?new DateTime(tk):DateTime.MinValue;
                var qua = block.ReadByte();
                re.Add(vid, new Tuple<object, DateTime, byte>(value,time, qua));
            }
            block.UnlockAndReturn();
            return re;
        }

        private Dictionary<int, Tuple<object,byte>> ProcessPushSingleBufferData(ByteBuffer block)
        {
            if (block == null||block.Length<1) return null;
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
            if (block == null || block.Length<1) return null;
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
            if (block == null || block.Length<1) return null;
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
        /// 获取变量实时值
        /// </summary>
        /// <param name="ids">变量Id集合</param>
        /// <param name="nocache">是否从缓存中读取，从缓冲中读取会提高访问速度，但是数值本身会有一定的滞后</param>
        /// <param name="timeout">超时缺省:5000</param>
        /// <returns>变量Id，值，时间,质量 的集合</returns>
        public Dictionary<int, Tuple<object, DateTime, byte>> GetRealData(List<int> ids,bool nocache=false, int timeout = 5000)
        {
            lock(mlockRealObj)
            return ProcessSingleBufferData(GetRealDataInner(ids, nocache, timeout));
        }

        /// <summary>
        /// 获取变量实时值
        /// </summary>
        /// <param name="ids">变量Id集合</param>
        /// <param name="nocache">是否从缓存中读取，从缓冲中读取会提高访问速度，但是数值本身会有一定的滞后</param>
        /// <param name="timeout">超时缺省:5000</param>
        /// <returns>变量Id，值</returns>
        public Dictionary<int, object> GetRealDataValueOnly(List<int> ids, bool nocache = false, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferDataValue(GetRealDataInnerValueOnly(ids,nocache, timeout));
        }

        /// <summary>
        /// 获取变量实时值、质量戳
        /// </summary>
        /// <param name="ids">变量Id集合</param>
        /// <param name="nocache">是否从缓存中读取，从缓冲中读取会提高访问速度，但是数值本身会有一定的滞后</param>
        /// <param name="timeout">超时缺省:5000</param>
        /// <returns>变量Id，值，质量 的集合</returns>
        public Dictionary<int, Tuple<object,byte>> GetRealDataValueAndQualityOnly(List<int> ids,bool nocache=false, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferDataValueAndQuality(GetRealDataInnerValueAndQualityOnly(ids,nocache, timeout));
        }

        /// <summary>
        /// 获取变量实时值，通过指定变量ID范围
        /// </summary>
        /// <param name="startId">开始变量的ID</param>
        /// <param name="endId">结束变量的ID</param>
        /// <param name="nocache">是否从缓存中读取，从缓冲中读取会提高访问速度，但是数值本身会有一定的滞后</param>
        /// <param name="timeout">超时缺省:5000</param>
        /// <returns>变量Id，值，时间,质量 的集合</returns>
        public Dictionary<int, Tuple<object, DateTime, byte>> GetRealData(int startId,int endId, bool nocache=false,int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferData(GetRealDataInner(startId,endId,nocache, timeout));
        }

        /// <summary>
        /// 获取变量实时值，通过指定变量ID范围
        /// </summary>
        /// <param name="startId">开始变量的ID</param>
        /// <param name="endId">结束变量的ID</param>
        /// <param name="nocache">是否从缓存中读取，从缓冲中读取会提高访问速度，但是数值本身会有一定的滞后</param>
        /// <param name="timeout">超时缺省:5000</param>
        /// <returns>变量Id，值</returns>
        public Dictionary<int, object> GetRealDataValueOnly(int startId, int endId,bool nocache=false, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferDataValue(GetRealDataInnerValueOnly(startId,endId,nocache, timeout));
        }

        /// <summary>
        /// 获取变量实时值，通过指定变量ID范围
        /// </summary>
        /// <param name="startId">开始变量的ID</param>
        /// <param name="endId">结束变量的ID</param>
        /// <param name="nocache">是否从缓存中读取，从缓冲中读取会提高访问速度，但是数值本身会有一定的滞后</param>
        /// <param name="timeout">超时缺省:5000</param>
        /// <returns>变量Id，值,质量 的集合</returns>

        public Dictionary<int, Tuple<object, byte>> GetRealDataValueAndQualityOnly(int startId, int endId,bool nocache=false, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferDataValueAndQuality(GetRealDataInnerValueAndQualityOnly(startId, endId,nocache, timeout));
        }


        /// <summary>
        /// 设置变量的值
        /// </summary>
        /// <param name="id">变量Id</param>
        /// <param name="valueType">值类型<see cref="Cdy.Tag.TagType"/></param>
        /// <param name="value">值</param>
        /// <param name="timeout">超时 缺省:5000 ms</param>
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
                    mRealRequreData?.UnlockAndReturn();
                    mRealRequreData = null;
                }
                return false;
            }
        }

        /// <summary>
        /// 设置一组变量的值
        /// </summary>
        /// <param name="id">变量ID集合</param>
        /// <param name="valueType">值类型集合</param>
        /// <param name="value">值集合</param>
        /// <param name="timeout">超时 缺省:5000 ms</param>
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
                    mRealRequreData?.UnlockAndReturn();
                    mRealRequreData = null;
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
        /// 查询某个变量的某个时间段内的记录的所有值
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="timeout">超时 缺省:5000 ms</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryAllHisValue<T>(int id,DateTime startTime,DateTime endTime,int timeout=5000)
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
                        return mHisRequreData.Convert<T>();
                    }
                }
                finally
                {
                    //mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
                return null;
            }
        }

        /// <summary>
        /// 查询某个变量在一系列时间点上得值
        /// </summary>
        /// <param name="id">变量Id</param>
        /// <param name="times">时间点集合</param>
        /// <param name="matchType">值拟合类型<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时 缺省:5000 ms</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueAtTimes<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
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
                        return mHisRequreData.Convert<T>();
                    }
                }
                finally
                {
                    //mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
                return null;
            }
        }

        /// <summary>
        /// 查询某个变量在一系列时间点上得值,通过指定时间间隔确定时间点
        /// </summary>
        /// <param name="id">变量Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="span">时间间隔<see cref="System.TimeSpan"/></param>
        /// <param name="matchType">值拟合类型<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时 缺省:5000ms</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueForTimeSpan<T>(int id,DateTime startTime,DateTime endTime,TimeSpan span,QueryValueMatchType matchType, int timeout = 5000)
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
                        return mHisRequreData.Convert<T>();
                    }
                }
                finally
                {
                    //mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
                return null;
            }
        }


        /// <summary>
        /// 查询某个变量某个时间段内的记录的统计值
        /// </summary>
        /// <param name="id">变量Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="timeout">超时 缺省:5000ms</param>
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
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
                return null;
            }
        }

        /// <summary>
        /// 查询某个变量在某个时刻上的记录的统计值
        /// </summary>
        /// <param name="id">变量Id</param>
        /// <param name="times">时间集合</param>
        /// <param name="timeout">超时 缺省:5000ms</param>
        /// <returns></returns>
        public NumberStatisticsQueryResult QueryStatisticsAtTimes(int id, List<DateTime> times, int timeout = 5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + times.Count * 8 + 5);
                mb.Write(ApiFunConst.RequestStatisticDataByTimeSpan);
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
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
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


        /// <summary>
        /// 查找某个值对应的时间
        /// </summary>
        /// <param name="id">变量的Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="type">值比较类型 <see cref="Cdy.Tag.NumberStatisticsType"/></param>
        /// <param name="value">值</param>
        /// <param name="timeout">超时 缺省:30000</param>
        /// <returns></returns>
        public Tuple<DateTime, object> FindTagValue(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, int timeout = 30000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                mb.Write(ApiFunConst.RequestFindTagValue);
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
                        byte cmd = mHisRequreData.ReadByte();
                        if (cmd == ApiFunConst.RequestFindTagValue)
                        {
                            TagType tp = (TagType)mHisRequreData.ReadByte();
                            byte res = mHisRequreData.ReadByte();
                            if (res == 1)
                            {
                                switch (tp)
                                {
                                    case TagType.DateTime:
                                    case TagType.Bool:
                                    case Cdy.Tag.TagType.String:
                                    case Cdy.Tag.TagType.IntPoint:
                                    case Cdy.Tag.TagType.UIntPoint:
                                    case Cdy.Tag.TagType.IntPoint3:
                                    case Cdy.Tag.TagType.UIntPoint3:
                                    case Cdy.Tag.TagType.LongPoint:
                                    case Cdy.Tag.TagType.ULongPoint:
                                    case Cdy.Tag.TagType.LongPoint3:
                                    case Cdy.Tag.TagType.ULongPoint3:
                                        return new Tuple<DateTime, object>(mHisRequreData.ReadDateTime(), 0);
                                    case Cdy.Tag.TagType.Double:
                                    case Cdy.Tag.TagType.Float:
                                    case Cdy.Tag.TagType.Byte:
                                    case Cdy.Tag.TagType.Int:
                                    case Cdy.Tag.TagType.Long:
                                    case Cdy.Tag.TagType.UInt:
                                    case Cdy.Tag.TagType.Short:
                                    case Cdy.Tag.TagType.ULong:
                                        return new Tuple<DateTime, object>(mHisRequreData.ReadDateTime(), mHisRequreData.ReadDouble());
                                }
                            }

                        }
                    }
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
                return null;
            }
        }

        /// <summary>
        /// 查找某个值保持的时间
        /// </summary>
        /// <param name="tag">变量的Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="type">值比较类型 <see cref="Cdy.Tag.NumberStatisticsType"/></param>
        /// <param name="value">值</param>
        /// <param name="timeout">超时 缺省:30000</param>
        /// <returns></returns>
        public double? CalTagValueKeepTime(int tag, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value,int timeout=30000)
        {
            lock (mlockHisQueryObj)
            {
                Dictionary<DateTime, object> re = new Dictionary<DateTime, object>();
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                mb.Write(ApiFunConst.RequestCalTagValueKeepTime);
                mb.Write(this.LoginId);
                mb.Write(tag);
                mb.Write(startTime.Ticks);
                mb.Write(endTime.Ticks);

                this.hisRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        byte cmd = mHisRequreData.ReadByte();
                        if (cmd == ApiFunConst.RequestCalTagValueKeepTime)
                        {
                            TagType tp = (TagType)mHisRequreData.ReadByte();
                            byte res = mHisRequreData.ReadByte();
                            if (res == 1)
                            {
                                return mHisRequreData.ReadDouble();
                            }

                        }
                    }
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
            }
            return null;
        }

        /// <summary>
        /// 查找某个值对应的时间
        /// </summary>
        /// <param name="tag">变量的Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="type">值比较类型 <see cref="Cdy.Tag.NumberStatisticsType"/></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public Dictionary<DateTime, object> FindTagValues(int tag, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, object interval, int timeout)
        {
            lock (mlockHisQueryObj)
            {
                Dictionary<DateTime, object> re = new Dictionary<DateTime, object>();
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                mb.Write(ApiFunConst.RequestFindTagValues);
                mb.Write(this.LoginId);
                mb.Write(tag);
                mb.Write(startTime.Ticks);
                mb.Write(endTime.Ticks);

                this.hisRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        byte cmd = mHisRequreData.ReadByte();
                        if (cmd == ApiFunConst.RequestFindTagValues)
                        {
                            TagType tp = (TagType)mHisRequreData.ReadByte();
                            byte res = mHisRequreData.ReadByte();
                            if (res == 1)
                            {
                                switch (tp)
                                {
                                    case TagType.DateTime:
                                    case TagType.Bool:
                                    case Cdy.Tag.TagType.String:
                                    case Cdy.Tag.TagType.IntPoint:
                                    case Cdy.Tag.TagType.UIntPoint:
                                    case Cdy.Tag.TagType.IntPoint3:
                                    case Cdy.Tag.TagType.UIntPoint3:
                                    case Cdy.Tag.TagType.LongPoint:
                                    case Cdy.Tag.TagType.ULongPoint:
                                    case Cdy.Tag.TagType.LongPoint3:
                                    case Cdy.Tag.TagType.ULongPoint3:
                                        int count = mHisRequreData.ReadInt();
                                        for (int i = 0; i < count; i++)
                                        {
                                            re.Add(mHisRequreData.ReadDateTime(), 0);
                                        }
                                        break;
                                    case Cdy.Tag.TagType.Double:
                                    case Cdy.Tag.TagType.Float:
                                    case Cdy.Tag.TagType.Byte:
                                    case Cdy.Tag.TagType.Int:
                                    case Cdy.Tag.TagType.Long:
                                    case Cdy.Tag.TagType.UInt:
                                    case Cdy.Tag.TagType.Short:
                                    case Cdy.Tag.TagType.ULong:
                                        count = mHisRequreData.ReadInt();
                                        for (int i = 0; i < count; i++)
                                        {
                                            re.Add(mHisRequreData.ReadDateTime(), mHisRequreData.ReadDouble());
                                        }
                                        break;
                                }
                            }

                        }
                    }
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
                return re;
            }
        }

        /// <summary>
        /// 查找最大值
        /// </summary>
        /// <param name="tag">变量的Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Tuple<double, List<DateTime>> FindNumberTagMaxValue(int tag, DateTime startTime, DateTime endTime, int timeout = 30000)
        {
            lock (mlockHisQueryObj)
            {
                List<DateTime> re = new List<DateTime>();
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                mb.Write(ApiFunConst.RequestFindNumberTagMaxValue);
                mb.Write(this.LoginId);
                mb.Write(tag);
                mb.Write(startTime.Ticks);
                mb.Write(endTime.Ticks);

                this.hisRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        byte cmd = mHisRequreData.ReadByte();
                        if (cmd == ApiFunConst.RequestFindNumberTagMaxValue)
                        {
                            TagType tp = (TagType)mHisRequreData.ReadByte();
                            byte res = mHisRequreData.ReadByte();
                            if (res == 1)
                            {
                                var dval = mHisRequreData.ReadDouble();
                                var count = mHisRequreData.ReadInt();
                                for (int i = 0; i < count; i++)
                                {
                                    re.Add(mHisRequreData.ReadDateTime());
                                }
                                return new Tuple<double, List<DateTime>>(dval, re);
                            }

                        }
                    }
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
            }
            return null;
        }

        /// <summary>
        /// 查找最小值
        /// </summary>
        /// <param name="tag">变量的Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Tuple<double, List<DateTime>> FindNumberTagMinValue(int tag, DateTime startTime, DateTime endTime,int timeout=30000)
        {
            lock (mlockHisQueryObj)
            {
                List<DateTime> re = new List<DateTime>();
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                mb.Write(ApiFunConst.RequestFindNumberTagMinValue);
                mb.Write(this.LoginId);
                mb.Write(tag);
                mb.Write(startTime.Ticks);
                mb.Write(endTime.Ticks);

                this.hisRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        byte cmd = mHisRequreData.ReadByte();
                        if (cmd == ApiFunConst.RequestFindNumberTagMinValue)
                        {
                            TagType tp = (TagType)mHisRequreData.ReadByte();
                            byte res = mHisRequreData.ReadByte();
                            if (res == 1)
                            {
                                var dval = mHisRequreData.ReadDouble();
                                var count = mHisRequreData.ReadInt();
                                for(int i=0;i<count;i++)
                                {
                                    re.Add(mHisRequreData.ReadDateTime());
                                }
                                return new Tuple<double, List<DateTime>>(dval, re);
                            }

                        }
                    }
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
            }
            return null;
        }

        /// <summary>
        /// 计算平均值
        /// </summary>
        /// <param name="tag">变量的Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public double? FindNumberTagAvgValue(int tag, DateTime startTime, DateTime endTime,int timeout=30000)
        {
            lock (mlockHisQueryObj)
            {
                Dictionary<DateTime, object> re = new Dictionary<DateTime, object>();
                CheckLogin();
                if (!IsLogin) return null;
                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                mb.Write(ApiFunConst.RequestCalNumberTagAvgValue);
                mb.Write(this.LoginId);
                mb.Write(tag);
                mb.Write(startTime.Ticks);
                mb.Write(endTime.Ticks);

                this.hisRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        byte cmd = mHisRequreData.ReadByte();
                        if (cmd == ApiFunConst.RequestCalNumberTagAvgValue)
                        {
                            TagType tp = (TagType)mHisRequreData.ReadByte();
                            byte res = mHisRequreData.ReadByte();
                            if (res == 1)
                            {
                                return mHisRequreData.ReadDouble();
                            }

                        }
                    }
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
            }
            return null;
        }

        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public static class ApiClientExtends
    {
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data"></param>
        /// <returns></returns>
        public static  HisQueryResult<T> Convert<T>(this ByteBuffer data)
        {
            int count = data.ReadInt();
            HisQueryResult<T> re = new HisQueryResult<T>(count);

            data.CopyTo(re.Address, data.ReadIndex, 0, data.WriteIndex - data.ReadIndex);

            re.Count = count;
            data.UnlockAndReturn();
            return re;
        }
    }
}
