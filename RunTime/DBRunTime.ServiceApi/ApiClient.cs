using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Cdy.Tag;
using DotNetty.Buffers;

namespace DBRunTime.ServiceApi
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

        public const byte RealDataSetFun = 11;

        /// <summary>
        /// 获取实时值
        /// </summary>
        public const byte RequestRealData = 0;

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

        
        public const byte SyncRealTagConfig = 30;

        public const byte SyncHisTagConfig = 31;

        public const byte SyncSecuritySetting = 32;


        /// <summary>
        /// 
        /// </summary>
        public const byte TagInfoNotify = 100;

        public const byte DatabaseChangedNotify = 1;


        public const byte AysncReturn = byte.MaxValue;
    }

    public class ApiClient:SocketClient
    {

        #region ... Variables  ...
        
        private ManualResetEvent infoRequreEvent = new ManualResetEvent(false);

        private ManualResetEvent SyncDataEvent = new ManualResetEvent(false);

        private IByteBuffer mInfoRequreData;

        private IByteBuffer mRealSyncData;

        private ManualResetEvent hisRequreEvent = new ManualResetEvent(false);

        private IByteBuffer mHisRequreData;

        private ManualResetEvent realRequreEvent = new ManualResetEvent(false);

        private ManualResetEvent realSetRequreEvent = new ManualResetEvent(false);

        private IByteBuffer mRealRequreData;

        private IByteBuffer mRealSetResponseData;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<byte, IByteBuffer> mReceivedDatas = new Dictionary<byte, IByteBuffer>();

        public delegate void ProcessDataPushDelegate(IByteBuffer datas);

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
        public long LoginId { get; set; }

        public ProcessDataPushDelegate ProcessDataPush { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Action<bool,bool,bool> DatabaseChangedAction { get; set; }

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
                ProcessDataPush?.Invoke(datas);
            }
            else if(fun == ApiFunConst.AysncReturn)
            {
                //收到异步请求回调数据
                return;
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
                    case ApiFunConst.RealDataSetFun:
                        mRealSetResponseData = datas;
                        this.realSetRequreEvent.Set();
                        break;
                    case ApiFunConst.HisDataRequestFun:
                        mHisRequreData = datas;
                        hisRequreEvent.Set();
                        break;
                    case ApiFunConst.SyncRealTagConfig:
                        mRealSyncData = datas;
                        SyncDataEvent.Set();
                        break;
                    case ApiFunConst.SyncSecuritySetting:
                        mRealSyncData = datas;
                        SyncDataEvent.Set();
                        break;
                    case ApiFunConst.SyncHisTagConfig:
                        mRealSyncData = datas;
                        SyncDataEvent.Set();
                        break;
                    case ApiFunConst.TagInfoNotify:
                        ProcessTagInfoNotify(datas);
                        break;
                    default:
                        Debug.Print("DbClient ProcessData Invailed data");
                        break;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        private void ProcessTagInfoNotify(IByteBuffer data)
        {
            byte cmd = data.ReadByte();
            switch(cmd)
            {
                case ApiFunConst.DatabaseChangedNotify:
                    var type = data.ReadByte();
                    DatabaseChangedAction?.BeginInvoke((type & 0x01) > 0, (type & 0x02) > 0, (type & 0x04) > 0, null, null);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        public bool Login(string username,string password,int timeount=5000)
        {
            mUser = username;
            mPass = password;
            int size = username.Length + password.Length + 9;
            var mb = GetBuffer(ApiFunConst.TagInfoRequest, size);
            mb.WriteByte(ApiFunConst.Login);
            mb.WriteString(username);
            mb.WriteString(password);
            Send(mb);
            if(infoRequreEvent.WaitOne(timeount)  && mInfoRequreData.ReadableBytes>4)
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public string GetRealdatabase(int timeout = 50000)
        {
            string filename = string.Empty;
            var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
            mb.WriteByte(ApiFunConst.SyncRealTagConfig);
            mb.WriteLong(LoginId);
            this.SyncDataEvent.Reset();
            Send(mb);

            if (SyncDataEvent.WaitOne(timeout))
            {
                if (this.mRealSyncData.ReadableBytes > 0)
                {
                    try
                    {
                        System.IO.MemoryStream ms = new System.IO.MemoryStream();

                        this.mRealSyncData.ReadBytes(ms, this.mRealSyncData.ReadableBytes);
                        ms.Position = 0;
                        System.IO.Compression.GZipStream gzip = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress);
                        filename = System.IO.Path.GetTempFileName();
                        var sfile = System.IO.File.Open(filename, System.IO.FileMode.OpenOrCreate);
                        gzip.CopyTo(sfile);
                        sfile.Close();

                        ms.Dispose();
                        gzip.Dispose();
                        return filename;
                    }
                    catch
                    {

                    }
                }
            }
            mRealSyncData?.Release();
            mRealSyncData = null;
            return filename;
        }


        public string GetSecuritySetting(int timeout = 50000)
        {
            string filename = string.Empty;
            var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
            mb.WriteByte(ApiFunConst.SyncSecuritySetting);
            mb.WriteLong(LoginId);
            this.SyncDataEvent.Reset();
            Send(mb);

            if (SyncDataEvent.WaitOne(timeout))
            {
                if (this.mRealSyncData.ReadableBytes > 0)
                {
                    try
                    {
                        System.IO.MemoryStream ms = new System.IO.MemoryStream();

                        this.mRealSyncData.ReadBytes(ms, this.mRealSyncData.ReadableBytes);
                        ms.Position = 0;
                        System.IO.Compression.GZipStream gzip = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress);
                        filename = System.IO.Path.GetTempFileName();
                        var sfile = System.IO.File.Open(filename, System.IO.FileMode.OpenOrCreate);
                        gzip.CopyTo(sfile);
                        sfile.Close();

                        ms.Dispose();
                        gzip.Dispose();
                        return filename;
                    }
                    catch
                    {

                    }
                }
            }
            mRealSyncData?.Release();
            mRealSyncData = null;

            return filename;
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
            CheckLogin();
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool RegistorTagBlockValueCallBack(int timeout = 5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun,8);
            mb.WriteByte(ApiFunConst.BlockValueChangeNotify);
            mb.WriteLong(this.LoginId);
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool ClearRegistorTagValueCallBack(int timeout = 5000)
        {
            CheckLogin();
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IByteBuffer GetRealData(List<int> ids,int timeout=5000)
        {
            CheckLogin();
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public IByteBuffer GetRealDataByMemoryCopy(List<int> ids, int timeout = 5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4);
            mb.WriteByte(ApiFunConst.RequestRealDataByMemoryCopy);
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
        public IByteBuffer GetRealData(int ids,int ide, int timeout = 5000)
        {
            CheckLogin();
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
        /// <param name="minid"></param>
        /// <param name="maxid"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public IEnumerable<IByteBuffer> SyncRealMemory(int startId,int endId,Func<int,int> getAddress, int timeout = 5000)
        {
            CheckLogin();
            int start = startId;
            int len = 1000000;
            while (start <= endId)
            {
                if (start + len > endId)
                {
                    len = endId - start;
                }

                int astart = getAddress(start);
                int aend = getAddress(start + len);

                if (len <= 0) break;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 4);
                mb.WriteByte(ApiFunConst.RealMemorySync);
                mb.WriteLong(this.LoginId);
                mb.WriteInt((aend - astart));
                mb.WriteInt(astart);
                realRequreEvent.Reset();
                Send(mb);
                if (realRequreEvent.WaitOne(timeout))
                {
                    if (mRealRequreData.ReadableBytes == (aend - astart))
                        yield return mRealRequreData;
                    else
                        yield return null;
                }
                else
                {
                    yield return null;
                }
                start += len;
            }
           
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="ide"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public IByteBuffer GetRealDataByMemoryCopy(int ids, int ide, int timeout = 5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + (ide - ids) * 4);
            mb.WriteByte(ApiFunConst.RequestRealData2ByMemoryCopy);
            mb.WriteLong(this.LoginId);
            mb.WriteInt(ids);
            mb.WriteInt(ide);
            realRequreEvent.Reset();
            Send(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData;
            }
            //mRealRequreData?.ReleaseBuffer();

            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="valueType"></param>
        /// <param name="value"></param>
        public bool SetTagValue(int id,byte valueType,object value,int timeout=5000)
        {
            CheckLogin();
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
            realSetRequreEvent.Reset();
            Send(mb);
            try
            {
                if (realSetRequreEvent.WaitOne(timeout))
                {
                    if (this.mRealSetResponseData.ReadableBytes > 0)
                    {
                        return mRealSetResponseData.ReadByte() > 0;
                    }
                    else
                    {
                        return true;
                    }
                }
            }
            finally
            {
                //mRealRequreData?.ReleaseBuffer();
            }
            return false;
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
            CheckLogin();
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


        #endregion

        private void CheckLogin()
        {
            if(LoginId<0)
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
            CheckLogin();
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="matchType"></param>
        /// <returns></returns>
        public IByteBuffer QueryHisValueAtTimes(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            CheckLogin();
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
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 24+ 5);
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

        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
