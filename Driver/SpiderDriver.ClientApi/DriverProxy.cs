using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Cdy.Tag;
using DotNetty.Buffers;

namespace SpiderDriver.ClientApi
{
    /// <summary>
    /// 
    /// </summary>
    public class DriverProxy:Cdy.Tag.SocketClient
    {

        #region ... Variables  ...
        
        private long mLoginId;

        private Queue<IByteBuffer> mInfoRequreData = new Queue<IByteBuffer>();

        private IByteBuffer mRealRequreData;

        private IByteBuffer mHisRequreData;

        private ManualResetEvent infoRequreEvent = new ManualResetEvent(false);

        private ManualResetEvent realRequreEvent = new ManualResetEvent(false);

        private ManualResetEvent hisRequreEvent = new ManualResetEvent(false);

        public delegate void ProcessDataPushDelegate(Dictionary<int, object> datas);

        private string mUser, mPass;


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public bool IsLogin { get { return mLoginId > 0; } }

        /// <summary>
        /// 
        /// </summary>
        public ProcessDataPushDelegate ValueChanged { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        protected override void OnConnectChanged(bool value)
        {
            if (!value) mLoginId = -1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="datas"></param>
        protected override void ProcessData(byte fun, IByteBuffer datas)
        {
            
            if (fun == ApiFunConst.PushDataChangedFun)
            {
                ValueChanged?.Invoke(ProcessSingleBufferData(datas));
            }
            else
            {
                datas.Retain();
                switch (fun)
                {
                    case ApiFunConst.TagInfoRequestFun:
                        lock (mInfoRequreData)
                            mInfoRequreData.Enqueue(datas);
                        infoRequreEvent.Set();
                        break;
                    case ApiFunConst.RealValueFun:
                        mRealRequreData = datas;
                        this.realRequreEvent.Set();
                        break;
                    case ApiFunConst.HisValueFun:
                        mHisRequreData = datas;
                        hisRequreEvent.Set();
                        break;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        private Dictionary<int, object> ProcessSingleBufferData(IByteBuffer block)
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
            //block.Release();
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="timeount"></param>
        /// <returns></returns>
        public bool Login(string username, string password, int timeount = 5000)
        {
             mUser = username;
             mPass = password;
            int size = username.Length + password.Length + 9;
            var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, size);
            mb.WriteByte(ApiFunConst.LoginFun);
            mb.WriteString(username);
            mb.WriteString(password);
            Send(mb);
            if (infoRequreEvent.WaitOne(timeount) && mInfoRequreData.Count>0)
            {
                var vdata = mInfoRequreData.Dequeue();
                
                if (vdata.ReadableBytes > 4 && vdata.ReferenceCount > 0)
                {
                    mLoginId = vdata.ReadLong();
                    return IsLogin;
                }
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="re"></param>
        private void SetTagValueToBuffer(TagType type,object value,IByteBuffer re)
        {
            re.WriteByte((byte)type);
            switch (type)
            {
                case TagType.Bool:
                    re.WriteByte((byte)value);
                    break;
                case TagType.Byte:
                    re.WriteByte((byte)value);
                    break;
                case TagType.Short:
                    re.WriteShort((short)value);
                    break;
                case TagType.UShort:
                    re.WriteUnsignedShort((ushort)value);
                    break;
                case TagType.Int:
                    re.WriteInt((int)value);
                    break;
                case TagType.UInt:
                    re.WriteInt((int)value);
                    break;
                case TagType.Long:
                case TagType.ULong:
                    re.WriteLong((long)value);
                    break;
                case TagType.Float:
                    re.WriteFloat((float)value);
                    break;
                case TagType.Double:
                    re.WriteDouble((double)value);
                    break;
                case TagType.String:
                    string sval = value.ToString();
                    re.WriteInt(sval.Length);
                    re.WriteString(sval, Encoding.Unicode);
                    break;
                case TagType.DateTime:
                    re.WriteLong(((DateTime)value).Ticks);
                    break;
                case TagType.IntPoint:
                    re.WriteInt(((IntPointData)value).X);
                    re.WriteInt(((IntPointData)value).Y);
                    break;
                case TagType.UIntPoint:
                    re.WriteInt((int)((UIntPointData)value).X);
                    re.WriteInt((int)((UIntPointData)value).Y);
                    break;
                case TagType.IntPoint3:
                    re.WriteInt(((IntPoint3Data)value).X);
                    re.WriteInt(((IntPoint3Data)value).Y);
                    re.WriteInt(((IntPoint3Data)value).Z);
                    break;
                case TagType.UIntPoint3:
                    re.WriteInt((int)((UIntPoint3Data)value).X);
                    re.WriteInt((int)((UIntPoint3Data)value).Y);
                    re.WriteInt((int)((UIntPoint3Data)value).Z);
                    break;
                case TagType.LongPoint:
                    re.WriteLong(((LongPointData)value).X);
                    re.WriteLong(((LongPointData)value).Y);
                    break;
                case TagType.ULongPoint:
                    re.WriteLong((long)((ULongPointData)value).X);
                    re.WriteLong((long)((ULongPointData)value).Y);
                    break;
                case TagType.LongPoint3:
                    re.WriteLong(((LongPoint3Data)value).X);
                    re.WriteLong(((LongPoint3Data)value).Y);
                    re.WriteLong(((LongPoint3Data)value).Z);
                    break;
                case TagType.ULongPoint3:
                    re.WriteLong((long)((ULongPoint3Data)value).X);
                    re.WriteLong((long)((ULongPoint3Data)value).Y);
                    re.WriteLong((long)((ULongPoint3Data)value).Z);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="re"></param>
        private void SetTagValueToBuffer2(TagType type, object value, IByteBuffer re)
        {
            switch (type)
            {
                case TagType.Bool:
                    re.WriteByte((byte)value);
                    break;
                case TagType.Byte:
                    re.WriteByte((byte)value);
                    break;
                case TagType.Short:
                    re.WriteShort((short)value);
                    break;
                case TagType.UShort:
                    re.WriteUnsignedShort((ushort)value);
                    break;
                case TagType.Int:
                    re.WriteInt((int)value);
                    break;
                case TagType.UInt:
                    re.WriteInt((int)value);
                    break;
                case TagType.Long:
                case TagType.ULong:
                    re.WriteLong((long)value);
                    break;
                case TagType.Float:
                    re.WriteFloat((float)value);
                    break;
                case TagType.Double:
                    re.WriteDouble((double)value);
                    break;
                case TagType.String:
                    string sval = value.ToString();
                    re.WriteInt(sval.Length);
                    re.WriteString(sval, Encoding.Unicode);
                    break;
                case TagType.DateTime:
                    re.WriteLong(((DateTime)value).Ticks);
                    break;
                case TagType.IntPoint:
                    re.WriteInt(((IntPointData)value).X);
                    re.WriteInt(((IntPointData)value).Y);
                    break;
                case TagType.UIntPoint:
                    re.WriteInt((int)((UIntPointData)value).X);
                    re.WriteInt((int)((UIntPointData)value).Y);
                    break;
                case TagType.IntPoint3:
                    re.WriteInt(((IntPoint3Data)value).X);
                    re.WriteInt(((IntPoint3Data)value).Y);
                    re.WriteInt(((IntPoint3Data)value).Z);
                    break;
                case TagType.UIntPoint3:
                    re.WriteInt((int)((UIntPoint3Data)value).X);
                    re.WriteInt((int)((UIntPoint3Data)value).Y);
                    re.WriteInt((int)((UIntPoint3Data)value).Z);
                    break;
                case TagType.LongPoint:
                    re.WriteLong(((LongPointData)value).X);
                    re.WriteLong(((LongPointData)value).Y);
                    break;
                case TagType.ULongPoint:
                    re.WriteLong((long)((ULongPointData)value).X);
                    re.WriteLong((long)((ULongPointData)value).Y);
                    break;
                case TagType.LongPoint3:
                    re.WriteLong(((LongPoint3Data)value).X);
                    re.WriteLong(((LongPoint3Data)value).Y);
                    re.WriteLong(((LongPoint3Data)value).Z);
                    break;
                case TagType.ULongPoint3:
                    re.WriteLong((long)((ULongPoint3Data)value).X);
                    re.WriteLong((long)((ULongPoint3Data)value).Y);
                    re.WriteLong((long)((ULongPoint3Data)value).Z);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="re"></param>
        private void SetTagValueToBuffer(TagType type, object value,byte quality, IByteBuffer re)
        {
            re.WriteByte((byte)type);
            switch (type)
            {
                case TagType.Bool:
                    re.WriteByte((byte)value);
                    break;
                case TagType.Byte:
                    re.WriteByte((byte)value);
                    break;
                case TagType.Short:
                    re.WriteShort((short)value);
                    break;
                case TagType.UShort:
                    re.WriteUnsignedShort((ushort)value);
                    break;
                case TagType.Int:
                    re.WriteInt((int)value);
                    break;
                case TagType.UInt:
                    re.WriteInt((int)value);
                    break;
                case TagType.Long:
                case TagType.ULong:
                    re.WriteLong((long)value);
                    break;
                case TagType.Float:
                    re.WriteFloat((float)value);
                    break;
                case TagType.Double:
                    re.WriteDouble((double)value);
                    break;
                case TagType.String:
                    string sval = value.ToString();
                    re.WriteInt(sval.Length);
                    re.WriteString(sval, Encoding.Unicode);
                    break;
                case TagType.DateTime:
                    re.WriteLong(((DateTime)value).Ticks);
                    break;
                case TagType.IntPoint:
                    re.WriteInt(((IntPointData)value).X);
                    re.WriteInt(((IntPointData)value).Y);
                    break;
                case TagType.UIntPoint:
                    re.WriteInt((int)((UIntPointData)value).X);
                    re.WriteInt((int)((UIntPointData)value).Y);
                    break;
                case TagType.IntPoint3:
                    re.WriteInt(((IntPoint3Data)value).X);
                    re.WriteInt(((IntPoint3Data)value).Y);
                    re.WriteInt(((IntPoint3Data)value).Z);
                    break;
                case TagType.UIntPoint3:
                    re.WriteInt((int)((UIntPoint3Data)value).X);
                    re.WriteInt((int)((UIntPoint3Data)value).Y);
                    re.WriteInt((int)((UIntPoint3Data)value).Z);
                    break;
                case TagType.LongPoint:
                    re.WriteLong(((LongPointData)value).X);
                    re.WriteLong(((LongPointData)value).Y);
                    break;
                case TagType.ULongPoint:
                    re.WriteLong((long)((ULongPointData)value).X);
                    re.WriteLong((long)((ULongPointData)value).Y);
                    break;
                case TagType.LongPoint3:
                    re.WriteLong(((LongPoint3Data)value).X);
                    re.WriteLong(((LongPoint3Data)value).Y);
                    re.WriteLong(((LongPoint3Data)value).Z);
                    break;
                case TagType.ULongPoint3:
                    re.WriteLong((long)((ULongPoint3Data)value).X);
                    re.WriteLong((long)((ULongPoint3Data)value).Y);
                    re.WriteLong((long)((ULongPoint3Data)value).Z);
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
        private void SetTagValueToBuffer2(TagType type, object value, byte quality, IByteBuffer re)
        {
            switch (type)
            {
                case TagType.Bool:
                    re.WriteByte((byte)value);
                    break;
                case TagType.Byte:
                    re.WriteByte((byte)value);
                    break;
                case TagType.Short:
                    re.WriteShort((short)value);
                    break;
                case TagType.UShort:
                    re.WriteUnsignedShort((ushort)value);
                    break;
                case TagType.Int:
                    re.WriteInt((int)value);
                    break;
                case TagType.UInt:
                    re.WriteInt((int)value);
                    break;
                case TagType.Long:
                case TagType.ULong:
                    re.WriteLong((long)value);
                    break;
                case TagType.Float:
                    re.WriteFloat((float)value);
                    break;
                case TagType.Double:
                    re.WriteDouble((double)value);
                    break;
                case TagType.String:
                    string sval = value.ToString();
                    re.WriteInt(sval.Length);
                    re.WriteString(sval, Encoding.Unicode);
                    break;
                case TagType.DateTime:
                    re.WriteLong(((DateTime)value).Ticks);
                    break;
                case TagType.IntPoint:
                    re.WriteInt(((IntPointData)value).X);
                    re.WriteInt(((IntPointData)value).Y);
                    break;
                case TagType.UIntPoint:
                    re.WriteInt((int)((UIntPointData)value).X);
                    re.WriteInt((int)((UIntPointData)value).Y);
                    break;
                case TagType.IntPoint3:
                    re.WriteInt(((IntPoint3Data)value).X);
                    re.WriteInt(((IntPoint3Data)value).Y);
                    re.WriteInt(((IntPoint3Data)value).Z);
                    break;
                case TagType.UIntPoint3:
                    re.WriteInt((int)((UIntPoint3Data)value).X);
                    re.WriteInt((int)((UIntPoint3Data)value).Y);
                    re.WriteInt((int)((UIntPoint3Data)value).Z);
                    break;
                case TagType.LongPoint:
                    re.WriteLong(((LongPointData)value).X);
                    re.WriteLong(((LongPointData)value).Y);
                    break;
                case TagType.ULongPoint:
                    re.WriteLong((long)((ULongPointData)value).X);
                    re.WriteLong((long)((ULongPointData)value).Y);
                    break;
                case TagType.LongPoint3:
                    re.WriteLong(((LongPoint3Data)value).X);
                    re.WriteLong(((LongPoint3Data)value).Y);
                    re.WriteLong(((LongPoint3Data)value).Z);
                    break;
                case TagType.ULongPoint3:
                    re.WriteLong((long)((ULongPoint3Data)value).X);
                    re.WriteLong((long)((ULongPoint3Data)value).Y);
                    re.WriteLong((long)((ULongPoint3Data)value).Z);
                    break;
            }
            re.WriteByte(quality);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValue(Dictionary<int, Tuple<TagType, object>> ids, int timeout = 5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.RealValueFun, 8 + ids.Count * 32);
            mb.WriteByte(ApiFunConst.SetTagValueFun);
            mb.WriteLong(this.mLoginId);
            mb.WriteInt(ids.Count);
            foreach (var vv in ids)
            {
                mb.WriteInt(vv.Key);
                SetTagValueToBuffer(vv.Value.Item1, vv.Value.Item2, mb);
            }
            realRequreEvent.Reset();
            Send(mb);

            try
            {
                if (realRequreEvent.WaitOne(timeout))
                {
                    return mRealRequreData != null && mRealRequreData.ReadableBytes>1;
                }
            }
            catch
            {

            }

            return false;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValue(Dictionary<int, Tuple<TagType, object,byte>> values, int timeout = 5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.RealValueFun, 8 + values.Count * 32);
            mb.WriteByte(ApiFunConst.SetTagValueAndQualityFun);
            mb.WriteLong(this.mLoginId);
            mb.WriteInt(values.Count);
            foreach (var vv in values)
            {
                mb.WriteInt(vv.Key);
                SetTagValueToBuffer(vv.Value.Item1, vv.Value.Item2,vv.Value.Item3, mb);
            }
            realRequreEvent.Reset();
            Send(mb);

            try
            {
                if (realRequreEvent.WaitOne(timeout))
                {
                    return mRealRequreData != null && mRealRequreData.ReadableBytes > 1;
                }
            }
            catch
            {

            }

            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool AppendRegistorDataChangedCallBack(List<int> ids,int timeout=5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.RealValueFun, 8 + ids.Count * 4);
            mb.WriteByte(ApiFunConst.RegistorTag);
            mb.WriteLong(this.mLoginId);
            mb.WriteInt(ids.Count);
            foreach (var vv in ids)
            {
                mb.WriteInt(vv);
            }
            realRequreEvent.Reset();
            Send(mb);

            try
            {
                if (realRequreEvent.WaitOne(timeout))
                {
                    return mRealRequreData != null && mRealRequreData.ReadableBytes > 1;
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool UnRegistorDataChangedCallBack(List<int> ids, int timeout = 5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.RealValueFun, 8 + ids.Count * 4);
            mb.WriteByte(ApiFunConst.UnRegistorTag);
            mb.WriteLong(this.mLoginId);
            mb.WriteInt(ids.Count);
            foreach (var vv in ids)
            {
                mb.WriteInt(vv);
            }
            realRequreEvent.Reset();
            Send(mb);

            try
            {
                if (realRequreEvent.WaitOne(timeout))
                {
                    return mRealRequreData != null && mRealRequreData.ReadableBytes > 1;
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool ResetRegistorDataChangedCallBack(int timeout = 5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.RealValueFun, 8);
            mb.WriteByte(ApiFunConst.ClearRegistorTag);
            mb.WriteLong(this.mLoginId);
            mb.WriteInt(1);
            realRequreEvent.Reset();
            Send(mb);
            try
            {
                if (realRequreEvent.WaitOne(timeout))
                {
                    return mRealRequreData != null && mRealRequreData.ReadableBytes > 1;
                }
            }
            catch
            {

            }
            return false;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="type"></param>
        /// <param name="values"></param>
        /// <param name="timeUnit"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagHisValue(int id, TagType type, List<TagValue> values, int timeout = 5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.HisValueFun, 22 + values.Count * 33);
            mb.WriteByte(ApiFunConst.SetTagHisValue);
            mb.WriteLong(this.mLoginId);
            mb.WriteInt(id);
            mb.WriteInt(values.Count);
            mb.WriteByte((byte)type);
            

            foreach (var vv in values)
            {
                mb.WriteLong(vv.Time.ToBinary());
                SetTagValueToBuffer2(type, vv.Value,vv.Quality, mb);
            }
            hisRequreEvent.Reset();
            Send(mb);

            try
            {
                if (this.hisRequreEvent.WaitOne(timeout))
                {
                    return mHisRequreData != null && mHisRequreData.ReadableBytes > 1;
                }
            }
            catch
            {

            }

            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="idvalues"></param>
        /// <param name="timeUnit"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagHisValue(Dictionary<int,TagValueAndType> idvalues,int timeout=5000)
        {
            if (idvalues == null && idvalues.Count == 0) return false;

            CheckLogin();
            var mb = GetBuffer(ApiFunConst.HisValueFun, 17 + idvalues.Count * 38);
            mb.WriteByte(ApiFunConst.SetTagHisValue2);
            mb.WriteLong(this.mLoginId);
            mb.WriteInt(idvalues.Count);

            foreach (var vv in idvalues)
            {
                mb.WriteInt(vv.Key);
                mb.WriteLong(vv.Value.Time.ToBinary());
                mb.WriteByte((byte)vv.Value.ValueType);
                SetTagValueToBuffer2(vv.Value.ValueType, vv.Value.Value, vv.Value.Quality, mb);
            }

            hisRequreEvent.Reset();
            Send(mb);

            try
            {
                if (this.hisRequreEvent.WaitOne(timeout))
                {
                    return mHisRequreData != null && mHisRequreData.ReadableBytes > 1;
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <param name="timeUnit"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagHisValue(int id, TagValueAndType values,  int timeout = 5000)
        {
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.HisValueFun, 17 + 38);
            mb.WriteByte(ApiFunConst.SetTagHisValue2);
            mb.WriteLong(this.mLoginId);
            mb.WriteInt(1);

            mb.WriteInt(id);
            mb.WriteLong(values.Time.ToBinary());
            mb.WriteByte((byte)values.ValueType);
            SetTagValueToBuffer2(values.ValueType, values.Value, values.Quality, mb);

            hisRequreEvent.Reset();
            Send(mb);

            try
            {
                if (this.hisRequreEvent.WaitOne(timeout))
                {
                    return mHisRequreData != null && mHisRequreData.ReadableBytes > 1;
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public List<int> QueryTagId(List<string> tags,int timeout = 5000)
        {
            List<int> re = new List<int>();
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 8 + tags.Count * 256);
            mb.WriteByte(ApiFunConst.GetTagIdByNameFun);
            mb.WriteLong(this.mLoginId);
            mb.WriteInt(tags.Count);
            foreach (var vv in tags)
            {
                mb.WriteString(vv);
            }
            infoRequreEvent.Reset();
            Send(mb);

            try
            {
                if (infoRequreEvent.WaitOne(timeout) && mInfoRequreData.Count > 0)
                {
                    var vdata = mInfoRequreData.Dequeue();
                    if(vdata != null)
                    {
                        var count = vdata.ReadInt();
                        for(int i=0;i<count;i++)
                        {
                            re.Add(vdata.ReadInt());
                        }
                    }
                }
            }
            catch
            {

            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<string, byte>> QueryAllTagIdAndNames(int timeout=5000)
        {
            Dictionary<int,Tuple<string,byte>> re = new Dictionary<int, Tuple<string, byte>>();
            CheckLogin();
            var mb = GetBuffer(ApiFunConst.TagInfoRequestFun, 8);
            mb.WriteByte(ApiFunConst.QueryAllTagNameAndIds);
            mb.WriteLong(this.mLoginId);
            DateTime dt = DateTime.Now;
            infoRequreEvent.Reset();
            lock(mInfoRequreData)
            mInfoRequreData.Clear();
            Send(mb);
            try
            {
                if (infoRequreEvent.WaitOne(timeout) && mInfoRequreData.Count > 0)
                {
                    while(true)
                    {
                        if (mInfoRequreData.Count > 0)
                        {
                            var vdata = mInfoRequreData.Peek();
                            var cmd = vdata.ReadByte();
                            if(cmd == ApiFunConst.QueryAllTagNameAndIds)
                            {
                                int total = vdata.ReadShort();
                                int icount = vdata.ReadShort();
                                int tcount = vdata.ReadInt();
                                for(int i=0;i<tcount;i++)
                                {
                                    var id = vdata.ReadInt();
                                    var name = vdata.ReadString();
                                    var type = vdata.ReadByte();
                                    if(!re.ContainsKey(id))
                                    {
                                        re.Add(id,new Tuple<string, byte>(name,type));
                                    }
                                }
                             
                                lock (mInfoRequreData)
                                    mInfoRequreData.Dequeue();

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

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
