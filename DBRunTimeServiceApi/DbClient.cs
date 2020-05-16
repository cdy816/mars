using System;
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

        /// <summary>
        /// 获取实时值
        /// </summary>
        public const byte RequestRealData = 0;

        public const byte RequestRealData2 = 10;

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
        public const byte RequestHisDataByTimePoint = 1;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDataByTimeSpan = 2;
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
        public bool IsLogin { get { return !string.IsNullOrEmpty(LoginId); } }

        /// <summary>
        /// 
        /// </summary>
        public string LoginId { get; set; }

        public ProcessDataPushDelegate ProcessDataPush { get; set; }

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
            else
            {
                datas.Retain();
                //收到异步请求回调数据
                if (datas.ReadableBytes==1&&datas.ReadByte()==byte.MaxValue)
                {
                    return;
                }
                else
                {
                    switch (fun)
                    {
                        case ApiFunConst.TagInfoRequest:
                            mInfoRequreData = datas ;
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
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        public bool Login(string username,string password,int timeount=5000)
        {
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
                    LoginId = mInfoRequreData.ReadString();
                    return IsLogin;
                }
            }
            mInfoRequreData?.Release();
            LoginId = string.Empty;
            return IsLogin;
        }

       

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagNames"></param>
        /// <returns></returns>
        public  Dictionary<string,int> GetTagIds( List<string> tagNames, int timeout = 5000)
        {
            Dictionary<string, int> re = new Dictionary<string, int>();

            var mb = GetBuffer(ApiFunConst.TagInfoRequest,tagNames.Count*24+1);
            mb.WriteByte(ApiFunConst.GetTagIdByNameFun);
            mb.WriteString(LoginId);
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
        public string GetRunnerDatabase(string loginid,int timeout = 5000)
        {
            var mb = GetBuffer(ApiFunConst.TagInfoRequest,loginid.Length+ 1);
            mb.WriteByte(ApiFunConst.GetRunnerDatabase);
            mb.WriteString(loginid);
            infoRequreEvent.Reset();
            Send(mb);

            if (infoRequreEvent.WaitOne(timeout))
            {
                return mInfoRequreData.ReadString();
            }
            
            mInfoRequreData?.ReleaseBuffer();
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
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, this.LoginId.Length + 8);
            mb.WriteByte(ApiFunConst.RegistorValueCallback);
            mb.WriteString(this.LoginId);
            mb.WriteInt(minid);
            mb.WriteInt(maxid);
            this.realRequreEvent.Reset();
            Send(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData.ReadByte() > 0;
            }
            mRealRequreData?.ReleaseBuffer();
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool ClearRegistorTagValueCallBack(int timeout = 5000)
        {
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, this.LoginId.Length + 8);
            mb.WriteByte(ApiFunConst.ResetValueChangeNotify);
            mb.WriteString(this.LoginId);
            realRequreEvent.Reset();
            Send(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData.ReadByte() > 0;
            }
            mRealRequreData?.ReleaseBuffer();
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IByteBuffer GetRealData(List<int> ids,int timeout=5000)
        {
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, this.LoginId.Length +ids.Count* 4);
            mb.WriteByte(ApiFunConst.RequestRealData);
            mb.WriteString(this.LoginId);
            mb.WriteInt(ids.Count);
            for(int i=0;i<ids.Count;i++)
            {
                mb.WriteInt(ids[i]);
            }
            realRequreEvent.Reset();
            Send(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData;
            }
            mRealRequreData?.Release();

            return null;
        }

        public IByteBuffer GetRealData(int ids,int ide, int timeout = 5000)
        {
         
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, this.LoginId.Length + (ide - ids) * 4);
            mb.WriteByte(ApiFunConst.RequestRealData2);
            mb.WriteString(this.LoginId);
            mb.WriteInt(ids);
            mb.WriteInt(ide);
            realRequreEvent.Reset();
            Send(mb);

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData;
            }
            mRealRequreData?.Release();

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
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, this.LoginId.Length + 30);
            mb.WriteByte(ApiFunConst.SetDataValue);
            mb.WriteString(this.LoginId);
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

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData.ReadByte()>0;
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
            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, this.LoginId.Length + 30);
            mb.WriteByte(ApiFunConst.SetDataValue);
            mb.WriteString(this.LoginId);
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

            if (realRequreEvent.WaitOne(timeout))
            {
                return mRealRequreData.ReadByte() > 0;
            }
            return false;
        }


        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
