﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Xml.Linq;
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

        public const byte Heart = 5;

        public const byte GetTagIdByNameFun = 0;

        public const byte Login = 1;

        public const byte RegistorValueCallback = 2;


        public const byte GetRunnerDatabase = 33;

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
        /// 按变量ID清空值改变通知2
        /// </summary>
        public const byte ResetValueChangeNotify2 = 23;


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

        /// <summary>
        /// 
        /// </summary>

        public const byte GetTagByFilter = 6;


        /// <summary>
        /// 修改历史数据
        /// </summary>
        public const byte ModifyHisData = 16;

        /// <summary>
        /// 删除历史数据
        /// </summary>
        public const byte DeleteHisData = 17;

        public const byte SetRealDataToLastData = 120;

        public const byte SetTagValueCallBack = 30;

        public const byte RequestComplexTagRealValue = 14;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDatasByTimePointIgnorSystemExit = 20;


        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDataByTimeSpanIgnorSystemExit = 22;


        /// <summary>
        /// 请求变量状态数据
        /// </summary>
        public const byte RequestStateData = 26;

        /// <summary>
        /// 请求变量扩展字段
        /// </summary>
        public const byte RequestExtendField2 = 27;

        /// <summary>
        /// 设置状态数据
        /// </summary>
        public const byte SetStateData = 28;

        /// <summary>
        /// 设置扩展数据
        /// </summary>
        public const byte SetExtendField2Data = 29;

        /// <summary>
        /// 通过SQL查询历史
        /// </summary>
        public const byte ReadHisValueBySQL = 140;

        public const byte TagStatisticsInfos = 7;
    }

    public class ApiClient:SocketClient2
    {

        #region ... Variables  ...
        
        private ManualResetEvent infoRequreEvent = new ManualResetEvent(false);

        private ByteBuffer mInfoRequreData;

        private ManualResetEvent hisRequreEvent = new ManualResetEvent(false);

        private ByteBuffer mHisRequreData;

        private ManualResetEvent realRequreEvent = new ManualResetEvent(false);

        private ManualResetEvent realSetRequreEvent = new ManualResetEvent(false);

        private ByteBuffer mRealRequreData;
        private ByteBuffer mRealSetRequreData;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<byte, ByteBuffer> mReceivedDatas = new Dictionary<byte, ByteBuffer>();

        public delegate void TagValueChangedCallBackDelegate(Dictionary<int, Tuple<object, byte,DateTime>> datas);

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

        /// <summary>
        /// 
        /// </summary>
        public string UserName
        {
            get
            {
                return mUser;
            }
            set
            {
                mUser = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string Password
        {
            get
            {
                return mPass;
            }
            set
            {
                mPass = value;
            }
        }


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
                    case ApiFunConst.RequestComplexTagRealValue:
                        mRealRequreData?.UnlockAndReturn();
                        mRealRequreData = datas;
                        this.realRequreEvent.Set();
                        break;
                    case ApiFunConst.SetTagValueCallBack:
                        mRealSetRequreData?.UnlockAndReturn();
                        mRealSetRequreData = datas;
                        realSetRequreEvent.Set();
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
                try
                {
                    mUser = username;
                    mPass = password;
                    var apphash = Cdy.Tag.Common.Common.Md5Helper.CalSha1();
                    int size = username.Length + password.Length + apphash.Length + 9;
                    var mb = GetBuffer(ApiFunConst.TagInfoRequest, size);
                    mb.Write(ApiFunConst.Login);
                    mb.Write(username);
                    mb.Write(password);
                    mb.Write(apphash);

                    infoRequreEvent.Reset();
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
                }
                catch(Exception ex)
                {
                    PrintErroMessage(ex);
                }
                return IsLogin;
            }
        }

        /// <summary>
        /// 登录
        /// </summary>
        /// <returns></returns>
        public bool Login(int timeout)
        {
            return Login(mUser, mPass,timeout);
        }


        /// <summary>
        /// 心跳
        /// </summary>
        public void Heart()
        {
            lock (mTagInfoObj)
            {
                try
                {
                    var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
                    mb.Write(ApiFunConst.Heart);
                    mb.Write(LoginId);
                    SendData(mb);
                }
                catch(Exception ex)
                {
                    PrintErroMessage(ex);
                }
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
                try
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
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
                try
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
                                                        if (vdata.Length == 2)
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
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }

                return re;
            }
        }

        /// <summary>
        /// 查询指定条件的变量
        /// </summary>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public Dictionary<int,Tuple<Cdy.Tag.Tagbase,Cdy.Tag.HisTag>> QueryTags(Dictionary<string,string> filters, out int totalCount, int skip=0,int take=0,int timeout = 30000)
        {
            Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> re = new Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>();
            totalCount = 0;
            lock (mTagInfoObj)
            {
                try
                {
                    StringBuilder sb = new StringBuilder();
                    foreach(var vv in filters)
                    {
                        sb.Append(vv.Key + ":" + vv.Value + ";");
                    }
                    if(take > 0)
                    {
                        sb.Append("Skip:"+skip+";");
                        sb.Append("Take:"+ take + ";");
                    }
                    if (sb.Length > 0)
                    {
                        sb.Length = sb.Length - 1;
                    }

                    var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9+sb.Length);
                    mb.Write(ApiFunConst.GetTagByFilter);
                    mb.Write(LoginId);
                    mb.Write(sb.ToString());
                    infoRequreEvent.Reset();
                    SendData(mb);

                    if (infoRequreEvent.WaitOne(timeout))
                    {
                        try
                        {
                            Dictionary<int, Cdy.Tag.Tagbase> ttmp = new Dictionary<int, Tagbase>();
                            Dictionary<int, Cdy.Tag.HisTag> htmp = new Dictionary<int, HisTag>();

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
                                                    XElement xe = XElement.Parse(sdata);
                                                    totalCount = int.Parse(xe.Attribute("TotalCount").Value);



                                                    foreach (var vv in xe.Element("RealTags").Elements())
                                                    {
                                                        var tag = vv.LoadTagFromXML();
                                                        ttmp.Add(tag.Id,tag);
                                                    }

                                                    foreach(var vv in xe.Element("HisTags").Elements())
                                                    {
                                                        var htag = vv.LoadHisTagFromXML();
                                                        htmp.Add(htag.Id,htag);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            foreach(var vv in ttmp)
                            {
                                re.Add(vv.Key, new Tuple<Tagbase, HisTag>(vv.Value, htmp.ContainsKey(vv.Key) ? htmp[vv.Key] : null));
                            }
                        }
                        catch
                        {

                        }
                        mInfoRequreData?.UnlockAndReturn();
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
                try
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
                                                var sdata = Encoding.Unicode.GetString(tms.GetBuffer(), 0, (int)tms.Position);
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
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
                try
                {
                    var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9 + 256);
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
                                                        if (vdata.Length == 2)
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
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
                
                try
                {
                    var mb = GetBuffer(ApiFunConst.TagInfoRequest, 8 + 1);
                    mb.Write(ApiFunConst.GetRunnerDatabase);
                    mb.Write(LoginId);
                    infoRequreEvent.Reset();
                    SendData(mb);

                    if (infoRequreEvent.WaitOne(timeout))
                    {
                        return mInfoRequreData.ReadString();
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    mInfoRequreData?.UnlockAndReturn();
                }
                return string.Empty;
            }
           
        }

        /// <summary>
        /// 获取变量统计信息
        /// </summary>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public Dictionary<string, int> GetTagStatisticsInfos(int timeout = 30000)
        {
            Dictionary<string, int> re = new Dictionary<string, int>();

            lock (mTagInfoObj)
            {
                try
                {
                    var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
                    mb.Write(ApiFunConst.TagStatisticsInfos);
                    mb.Write(LoginId);
                    infoRequreEvent.Reset();
                    SendData(mb);

                    if (infoRequreEvent.WaitOne(timeout))
                    {
                        try
                        {
                            if (mInfoRequreData != null && (mInfoRequreData.WriteIndex - mInfoRequreData.ReadIndex) > 4)
                            {
                                byte cmd = mInfoRequreData.ReadByte();
                                if(cmd != ApiFunConst.TagStatisticsInfos) return null;

                                int count = mInfoRequreData.ReadInt();
                                if (count > 0)
                                {
                                    for(int i=0;i < count;i++)
                                    {
                                        re.Add(mInfoRequreData.ReadString(),mInfoRequreData.ReadInt());
                                    }
                                }
                            }
                        }
                        catch
                        {

                        }
                        mInfoRequreData?.UnlockAndReturn();
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }

                return re;
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
                
                try
                {
                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
                    mb.Write(ApiFunConst.RegistorValueCallback);
                    mb.Write(this.LoginId);
                    mb.Write(minid);
                    mb.Write(maxid);
                    this.realRequreEvent.Reset();
                    SendData(mb);

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        return mRealRequreData.ReadByte() > 0;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
               
                try
                {
                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 1 + 4 + ids.Count * 4);
                    mb.Write(ApiFunConst.RegistorValueCallback);
                    mb.Write(this.LoginId);
                    mb.Write(ids.Count);
                    foreach (var vv in ids)
                    {
                        mb.Write(vv);
                    }

                    this.realRequreEvent.Reset();
                    SendData(mb);

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        return mRealRequreData.ReadByte() > 0;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
                try
                {
                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
                    mb.Write(ApiFunConst.ResetValueChangeNotify);
                    mb.Write(this.LoginId);
                    realRequreEvent.Reset();
                    SendData(mb);

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        return mRealRequreData.ReadByte() > 0;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
        /// <param name="ids">变量ID集合</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public bool ClearRegistorTagValueCallBack(List<int> ids, int timeout = 5000)
        {
            lock (mlockRealObj)
            {
                CheckLogin();
                if (!IsLogin) return false;

                try
                {
                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 1 + 4 + ids.Count * 4);
                    mb.Write(ApiFunConst.ResetValueChangeNotify2);
                    mb.Write(this.LoginId);
                    mb.Write(ids.Count);
                    foreach (var vv in ids)
                    {
                        mb.Write(vv);
                    }

                    this.realRequreEvent.Reset();
                    SendData(mb);

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        return mRealRequreData.ReadByte() > 0;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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

            lock (mlockRealObj)
            {
                try
                {
                    if (mRealRequreData != null)
                    {
                        RelaseBlock(mRealRequreData);
                        mRealRequreData = null;
                    }

                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4 + 1);
                    mb.Write(ApiFunConst.RequestRealData);
                    mb.Write(this.LoginId);
                    mb.Write(ids.Count);
                    for (int i = 0; i < ids.Count; i++)
                    {
                        mb.Write(ids[i]);
                    }
                    mb.Write(Convert.ToByte(nocach));
                    realRequreEvent.Reset();
                    SendData(mb);

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        mRealRequreData.IncRef();
                        return mRealRequreData;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    //mRealRequreData?.ReleaseBuffer();
                }
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
            lock (mlockRealObj)
            {
                try
                {

                    if (mRealRequreData != null)
                    {
                        RelaseBlock(mRealRequreData);
                        mRealRequreData = null;
                    }


                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4 + 1);
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

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        mRealRequreData.IncRef();
                        return mRealRequreData;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    //mRealRequreData?.ReleaseBuffer();
                }
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
            lock (mlockRealObj)
            {
                try
                {
                    if (mRealRequreData != null)
                    {
                        RelaseBlock(mRealRequreData);
                        mRealRequreData = null;
                    }

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

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        mRealRequreData.IncRef();
                        return mRealRequreData;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    //mRealRequreData?.ReleaseBuffer();
                }

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
        private ByteBuffer GetRealDataInnerValueAndQualityOnly(IEnumerable<int> ids, bool nocach = false, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            lock (mlockRealObj)
            {
                try
                {
                    if (mRealRequreData != null)
                    {
                        RelaseBlock(mRealRequreData);
                        mRealRequreData = null;
                    }

                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count() * 4);
                    mb.Write(ApiFunConst.RequestRealDataValueAndQuality);
                    mb.Write(this.LoginId);
                    mb.Write(ids.Count());
                    foreach(var vv in ids)
                    {
                        mb.Write(vv);
                    }
                    mb.Write(Convert.ToByte(nocach));
                    realRequreEvent.Reset();
                    SendData(mb);

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        mRealRequreData.IncRef();
                        return mRealRequreData;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    //mRealRequreData?.ReleaseBuffer();
                }

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
            lock (mlockRealObj)
            {
                try
                {
                    if (mRealRequreData != null)
                    {
                        RelaseBlock(mRealRequreData);
                        mRealRequreData = null;
                    }

                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8 + 1);
                    mb.Write(ApiFunConst.RequestRealData2);
                    mb.Write(this.LoginId);
                    mb.Write(ids);
                    mb.Write(ide);
                    mb.Write(Convert.ToByte(nocach));
                    realRequreEvent.Reset();
                    SendData(mb);

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        mRealRequreData.IncRef();
                        return mRealRequreData;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {

                }
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
        private ByteBuffer GetComplexTagRealValueInner(int ids, bool nocach = false, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            lock (mlockRealObj)
            {
                try
                {
                    if (mRealRequreData != null)
                    {
                        RelaseBlock(mRealRequreData);
                        mRealRequreData = null;
                    }

                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8 + 1);
                    mb.Write(ApiFunConst.RequestComplexTagRealValue);
                    mb.Write(this.LoginId);
                    mb.Write(ids);
                    mb.Write(Convert.ToByte(nocach));
                    realRequreEvent.Reset();
                    SendData(mb);

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        mRealRequreData.IncRef();
                        return mRealRequreData;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {

                }
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
            lock (mlockRealObj)
            {
                try
                {
                    if (mRealRequreData != null)
                    {
                        RelaseBlock(mRealRequreData);
                        mRealRequreData = null;
                    }

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
                        mRealRequreData.IncRef();
                        return mRealRequreData;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {

                }
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
            lock (mlockRealObj)
            {
                try
                {
                    if (mRealRequreData != null)
                    {
                        RelaseBlock(mRealRequreData);
                        mRealRequreData = null;
                    }
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
                        mRealRequreData.IncRef();
                        return mRealRequreData;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {

                }
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
        private Dictionary<int, Tuple<object, DateTime, byte>> ProcessSingleBufferData(ByteBuffer block)
        {
            if (block == null || block.Length < 1)
            {
                return null;
            }
            Dictionary<int, Tuple<object, DateTime, byte>> re = new Dictionary<int, Tuple<object, DateTime, byte>>();


            try
            {
                byte cmd = block.ReadByte();
                if (cmd != ApiFunConst.RequestRealData && cmd != ApiFunConst.RequestRealData2 && cmd!=ApiFunConst.RequestComplexTagRealValue) return null;

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
                        case (byte)TagType.Complex:
                            ComplexRealValue cv = new ComplexRealValue();
                            int vcount = block.ReadInt();
                            for (int j = 0; j < vcount; j++)
                            {
                                var cvid = block.ReadInt();
                                var vtp = block.ReadByte();
                                object cval = null;
                                switch (vtp)
                                {
                                    case (byte)TagType.Bool:
                                        cval = ((byte)block.ReadByte());
                                        break;
                                    case (byte)TagType.Byte:
                                        cval = ((byte)block.ReadByte());
                                        break;
                                    case (byte)TagType.Short:
                                        cval = (block.ReadShort());
                                        break;
                                    case (byte)TagType.UShort:
                                        cval = (block.ReadUShort());
                                        break;
                                    case (byte)TagType.Int:
                                        cval = (block.ReadInt());
                                        break;
                                    case (byte)TagType.UInt:
                                        cval = (block.ReadUInt());
                                        break;
                                    case (byte)TagType.Long:
                                    case (byte)TagType.ULong:
                                        cval = (block.ReadULong());
                                        break;
                                    case (byte)TagType.Float:
                                        cval = (block.ReadFloat());
                                        break;
                                    case (byte)TagType.Double:
                                        cval = (block.ReadDouble());
                                        break;
                                    case (byte)TagType.String:
                                        cval = block.ReadString();
                                        break;
                                    case (byte)TagType.DateTime:
                                        cval = DateTime.FromBinary(block.ReadLong());
                                        break;
                                    case (byte)TagType.IntPoint:
                                        cval = new IntPointData(block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint:
                                        cval = new UIntPointData(block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.IntPoint3:
                                        cval = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint3:
                                        cval = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.LongPoint:
                                        cval = new LongPointData(block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint:
                                        cval = new ULongPointData(block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.LongPoint3:
                                        cval = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint3:
                                        cval = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                                        break;
                                }
                                var ctime = DateTime.FromBinary(block.ReadLong()).ToLocalTime();
                                var cqua = block.ReadByte();
                                cv.Add(new ComplexRealValueItem() { Id = cvid, ValueType = vtp, Value = cval, Quality = cqua, Time = ctime });
                            }
                            value = cv;
                            break;
                    }
                    var tk = block.ReadLong();
                    var time = (tk > 0 && tk < DateTime.MaxValue.Ticks) ? new DateTime(tk).ToLocalTime() : DateTime.MinValue;
                    var qua = block.ReadByte();
                    re.Add(vid, new Tuple<object, DateTime, byte>(value, time, qua));
                }
            }
            catch
            {

            }
            finally
            {
                RelaseBlock(block);

            }

            return re;
        }


        private Dictionary<int, Tuple<object, byte,DateTime>> ProcessPushSingleBufferData(ByteBuffer block)
        {
            if (block == null || block.Length < 1)
            {
                RelaseBlock(block);
                return null;
            }
            Dictionary<int, Tuple<object, byte, DateTime>> re = new Dictionary<int, Tuple<object, byte, DateTime>>();
            try
            {
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
                        case (byte)TagType.Complex:
                            ComplexRealValue cv = new ComplexRealValue();
                            int vcount = block.ReadInt();
                            for (int j = 0; j < vcount; j++)
                            {
                                var cvid = block.ReadInt();
                                var vtp = block.ReadByte();
                                object cval = null;
                                switch (vtp)
                                {
                                    case (byte)TagType.Bool:
                                        cval = ((byte)block.ReadByte());
                                        break;
                                    case (byte)TagType.Byte:
                                        cval = ((byte)block.ReadByte());
                                        break;
                                    case (byte)TagType.Short:
                                        cval = (block.ReadShort());
                                        break;
                                    case (byte)TagType.UShort:
                                        cval = (block.ReadUShort());
                                        break;
                                    case (byte)TagType.Int:
                                        cval = (block.ReadInt());
                                        break;
                                    case (byte)TagType.UInt:
                                        cval = (block.ReadUInt());
                                        break;
                                    case (byte)TagType.Long:
                                    case (byte)TagType.ULong:
                                        cval = (block.ReadULong());
                                        break;
                                    case (byte)TagType.Float:
                                        cval = (block.ReadFloat());
                                        break;
                                    case (byte)TagType.Double:
                                        cval = (block.ReadDouble());
                                        break;
                                    case (byte)TagType.String:
                                        cval = block.ReadString();
                                        break;
                                    case (byte)TagType.DateTime:
                                        cval = DateTime.FromBinary(block.ReadLong());
                                        break;
                                    case (byte)TagType.IntPoint:
                                        cval = new IntPointData(block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint:
                                        cval = new UIntPointData(block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.IntPoint3:
                                        cval = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint3:
                                        cval = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.LongPoint:
                                        cval = new LongPointData(block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint:
                                        cval = new ULongPointData(block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.LongPoint3:
                                        cval = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint3:
                                        cval = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                                        break;
                                }
                                var ctime = DateTime.FromBinary(block.ReadLong()).ToLocalTime();
                                var cqua = block.ReadByte();
                                cv.Add(new ComplexRealValueItem() { Id = cvid, ValueType = vtp, Value = cval, Quality = cqua, Time = ctime });
                            }
                            value = cv;
                            break;
                    }
                    var qua = block.ReadByte();
                    var time = DateTime.FromBinary(block.ReadLong()).ToLocalTime();
                    re.Add(vid, new Tuple<object, byte,DateTime>(value, qua,time));
                }
            }
            catch
            {

            }
            finally
            {
                RelaseBlock(block);
            }

            return re;
        }


        private Dictionary<int, Tuple<object, byte>> ProcessSingleBufferDataValueAndQuality(ByteBuffer block)
        {
            if (block == null || block.Length < 1)
            {
                 RelaseBlock(block);
                return null;
            }
            Dictionary<int, Tuple<object,  byte>> re = new Dictionary<int, Tuple<object,  byte>>();
            
            try
            {
                byte cmd = block.ReadByte();
                if (cmd != ApiFunConst.RequestRealDataValueAndQuality && cmd != ApiFunConst.RequestReal2DataValueAndQuality) return null;
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
                        case (byte)TagType.Complex:
                            ComplexRealValue cv = new ComplexRealValue();
                            int vcount = block.ReadInt();
                            for (int j = 0; j < vcount; j++)
                            {
                                var cvid = block.ReadInt();
                                var vtp = block.ReadByte();
                                object cval = null;
                                switch (vtp)
                                {
                                    case (byte)TagType.Bool:
                                        cval = ((byte)block.ReadByte());
                                        break;
                                    case (byte)TagType.Byte:
                                        cval = ((byte)block.ReadByte());
                                        break;
                                    case (byte)TagType.Short:
                                        cval = (block.ReadShort());
                                        break;
                                    case (byte)TagType.UShort:
                                        cval = (block.ReadUShort());
                                        break;
                                    case (byte)TagType.Int:
                                        cval = (block.ReadInt());
                                        break;
                                    case (byte)TagType.UInt:
                                        cval = (block.ReadUInt());
                                        break;
                                    case (byte)TagType.Long:
                                    case (byte)TagType.ULong:
                                        cval = (block.ReadULong());
                                        break;
                                    case (byte)TagType.Float:
                                        cval = (block.ReadFloat());
                                        break;
                                    case (byte)TagType.Double:
                                        cval = (block.ReadDouble());
                                        break;
                                    case (byte)TagType.String:
                                        cval = block.ReadString();
                                        break;
                                    case (byte)TagType.DateTime:
                                        cval = DateTime.FromBinary(block.ReadLong());
                                        break;
                                    case (byte)TagType.IntPoint:
                                        cval = new IntPointData(block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint:
                                        cval = new UIntPointData(block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.IntPoint3:
                                        cval = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint3:
                                        cval = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.LongPoint:
                                        cval = new LongPointData(block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint:
                                        cval = new ULongPointData(block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.LongPoint3:
                                        cval = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint3:
                                        cval = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                                        break;
                                }
                                var ctime = DateTime.FromBinary(block.ReadLong()).ToLocalTime();
                                var cqua = block.ReadByte();
                                cv.Add(new ComplexRealValueItem() { Id = cvid, ValueType = vtp, Value = cval, Quality = cqua, Time = ctime });
                            }
                            value = cv;
                            break;
                    }
                    var qua = block.ReadByte();
                    re.Add(vid, new Tuple<object, byte>(value, qua));
                }
            }
            catch
            {

            }
            finally
            {
                RelaseBlock(block);
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        private void RelaseBlock(ByteBuffer block)
        {
            if(block == null) return;
            while (block.RefCount > 0)
            {
                block.DecRef();
            }
            block.UnlockAndReturn();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        private Dictionary<int, object> ProcessSingleBufferDataValue(ByteBuffer block)
        {
            if (block == null || block.Length < 1)
            {
                RelaseBlock(block);
                return null;
            }
            Dictionary<int, object> re = new Dictionary<int, object>();
           
            try
            {
                byte cmd = block.ReadByte();
                if (cmd != ApiFunConst.RequestRealDataValue && cmd != ApiFunConst.RequestReal2DataValue) return null;

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
                        case (byte)TagType.Complex:
                            ComplexRealValue cv = new ComplexRealValue();
                            int vcount = block.ReadInt();
                            for (int j = 0; j < vcount; j++) 
                            {
                                var cvid = block.ReadInt();
                                var vtp = block.ReadByte();
                                object cval = null;
                                switch (vtp)
                                {
                                    case (byte)TagType.Bool:
                                        cval = ((byte)block.ReadByte());
                                        break;
                                    case (byte)TagType.Byte:
                                        cval = ((byte)block.ReadByte());
                                        break;
                                    case (byte)TagType.Short:
                                        cval = (block.ReadShort());
                                        break;
                                    case (byte)TagType.UShort:
                                        cval = (block.ReadUShort());
                                        break;
                                    case (byte)TagType.Int:
                                        cval = (block.ReadInt());
                                        break;
                                    case (byte)TagType.UInt:
                                        cval = (block.ReadUInt());
                                        break;
                                    case (byte)TagType.Long:
                                    case (byte)TagType.ULong:
                                        cval = (block.ReadULong());
                                        break;
                                    case (byte)TagType.Float:
                                        cval = (block.ReadFloat());
                                        break;
                                    case (byte)TagType.Double:
                                        cval = (block.ReadDouble());
                                        break;
                                    case (byte)TagType.String:
                                        cval = block.ReadString();
                                        break;
                                    case (byte)TagType.DateTime:
                                        cval = DateTime.FromBinary(block.ReadLong());
                                        break;
                                    case (byte)TagType.IntPoint:
                                        cval = new IntPointData(block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint:
                                        cval = new UIntPointData(block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.IntPoint3:
                                        cval = new IntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.UIntPoint3:
                                        cval = new UIntPoint3Data(block.ReadInt(), block.ReadInt(), block.ReadInt());
                                        break;
                                    case (byte)TagType.LongPoint:
                                        cval = new LongPointData(block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint:
                                        cval = new ULongPointData(block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.LongPoint3:
                                        cval = new LongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                                        break;
                                    case (byte)TagType.ULongPoint3:
                                        cval = new ULongPoint3Data(block.ReadLong(), block.ReadLong(), block.ReadLong());
                                        break;
                                }
                                var ctime = DateTime.FromBinary(block.ReadLong()).ToLocalTime();
                                var cqua = block.ReadByte();
                                cv.Add(new ComplexRealValueItem() { Id = cvid, ValueType = vtp, Value = cval, Quality = cqua, Time = ctime });
                            }
                            value = cv;
                            break;
                    }
                    re.Add(vid, value);
                }
            }
            catch
            {

            }
            finally
            {
                RelaseBlock(block);
            }
            
            return re;
        }

        /// <summary>
        /// 获取变量的状态值
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="ide"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int, short> GetTagState(List<int> ids, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            lock (mlockRealObj)
            {
                try
                {
                    if (mRealRequreData != null)
                    {
                        RelaseBlock(mRealRequreData);
                        mRealRequreData = null;
                    }

                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, ids.Count*4 + 8 +4+ 2);
                    mb.Write(ApiFunConst.RequestStateData);
                    mb.Write(this.LoginId);
                    mb.Write(ids.Count);
                    foreach(var vv in ids)
                    {
                        mb.Write(vv);
                    }
                    realRequreEvent.Reset();
                    SendData(mb);

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        if( mRealRequreData.Length > 0 )
                        {
                            byte fun  = mRealRequreData.ReadByte();
                            if(fun == ApiFunConst.RequestStateData)
                            {
                                Dictionary<int,short> re = new Dictionary<int, short>();
                                int count = mRealRequreData.ReadInt();
                                for(int i=0;i<count;i++)
                                {
                                    re.Add(mRealRequreData.ReadInt(),mRealRequreData.ReadShort());
                                }
                                return re;
                            }
                        }
                       


                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {

                }
            }
            return null;
        }

        /// <summary>
        /// 获取变量的扩展属性2
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="ide"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int, long> GetTagExtendField2(List<int> ids, int timeout = 5000)
        {
            CheckLogin();
            if (!IsLogin) return null;
            lock (mlockRealObj)
            {
                try
                {
                    if (mRealRequreData != null)
                    {
                        RelaseBlock(mRealRequreData);
                        mRealRequreData = null;
                    }

                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, ids.Count * 4 + 8 + 4 + 2);
                    mb.Write(ApiFunConst.RequestExtendField2);
                    mb.Write(this.LoginId);
                    mb.Write(ids.Count);
                    foreach (var vv in ids)
                    {
                        mb.Write(vv);
                    }
                    realRequreEvent.Reset();
                    SendData(mb);

                    if (realRequreEvent.WaitOne(timeout))
                    {
                        if (mRealRequreData.Length > 0)
                        {
                            byte fun = mRealRequreData.ReadByte();
                            if (fun == ApiFunConst.RequestExtendField2)
                            {
                                Dictionary<int, long> re = new Dictionary<int, long>();
                                int count = mRealRequreData.ReadInt();
                                for (int i = 0; i < count; i++)
                                {
                                    re.Add(mRealRequreData.ReadInt(),mRealRequreData.ReadLong());
                                }
                                return re;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {

                }
            }
            return null;
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
        /// 获取变量实时值、质量戳
        /// </summary>
        /// <param name="ids">变量Id集合</param>
        /// <param name="nocache">是否从缓存中读取，从缓冲中读取会提高访问速度，但是数值本身会有一定的滞后</param>
        /// <param name="timeout">超时缺省:5000</param>
        /// <returns>变量Id，值，质量 的集合</returns>
        public Dictionary<int, Tuple<object, byte>> GetRealDataValueAndQualityOnly(IEnumerable<int> ids, bool nocache = false, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferDataValueAndQuality(GetRealDataInnerValueAndQualityOnly(ids, nocache, timeout));
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
        /// 获取通过模板类生成的复杂类型变量的子变量集合的实时值
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="nocache">是否从缓存中读取，从缓冲中读取会提高访问速度，但是数值本身会有一定的滞后</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public Dictionary<int, Tuple<object, DateTime, byte>> GetComplextTagRealData(int id, bool nocache = false, int timeout = 5000)
        {
            lock (mlockRealObj)
                return ProcessSingleBufferData(GetComplexTagRealValueInner(id, nocache, timeout));
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
               
                try
                {
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
                    realSetRequreEvent.Reset();
                    SendData(mb);

                    if (realSetRequreEvent.WaitOne(timeout))
                    {
                        return mRealSetRequreData.ReadByte() > 0;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    mRealSetRequreData?.UnlockAndReturn();
                    mRealSetRequreData = null;
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
               
                try
                {
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
                    realSetRequreEvent.Reset();
                    SendData(mb);

                    if (realSetRequreEvent.WaitOne(timeout))
                    {
                        return mRealSetRequreData.ReadByte() > 0;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    mRealSetRequreData?.UnlockAndReturn();
                    mRealSetRequreData = null;
                }
                return false;
            }
        }

        /// <summary>
        /// 设置变量的状态
        /// </summary>
        /// <param name="value">值</param>
        /// <param name="timeout">超时 缺省:5000 ms</param>
        public bool SetTagState(Dictionary<int,short> value, int timeout = 5000)
        {
            lock (mlockRealSetObj)
            {
                CheckLogin();
                if (!IsLogin) return false;

                try
                {
                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, value.Count*6 + 8 + 6);
                    mb.Write(ApiFunConst.SetStateData);
                    mb.Write(this.LoginId);
                    mb.Write(value.Count);
                   
                    foreach(var item in value)
                    {
                        mb.Write(item.Key);
                        mb.Write(item.Value);
                    }
                    realSetRequreEvent.Reset();
                    SendData(mb);

                    if (realSetRequreEvent.WaitOne(timeout))
                    {
                        return mRealSetRequreData.ReadByte() > 0;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    mRealSetRequreData?.UnlockAndReturn();
                    mRealSetRequreData = null;
                }
                return false;
            }
        }

        /// <summary>
        /// 设置变量的扩展属性2
        /// </summary>
        /// <param name="value">值</param>
        /// <param name="timeout">超时 缺省:5000 ms</param>
        public bool SetTagExtendField2(Dictionary<int, long> value, int timeout = 5000)
        {
            lock (mlockRealSetObj)
            {
                CheckLogin();
                if (!IsLogin) return false;

                try
                {
                    var mb = GetBuffer(ApiFunConst.RealDataRequestFun, value.Count * 12 + 8 + 6);
                    mb.Write(ApiFunConst.SetExtendField2Data);
                    mb.Write(this.LoginId);
                    mb.Write(value.Count);

                    foreach (var item in value)
                    {
                        mb.Write(item.Key);
                        mb.Write(item.Value);
                    }
                    realSetRequreEvent.Reset();
                    SendData(mb);

                    if (realSetRequreEvent.WaitOne(timeout))
                    {
                        return mRealSetRequreData.ReadByte() > 0;
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    mRealSetRequreData?.UnlockAndReturn();
                    mRealSetRequreData = null;
                }
                return false;
            }
        }
        ///// <summary>
        ///// 设置变量的值修改成上一次变更的有效值
        ///// </summary>
        ///// <param name="id">变量Id</param>
        ///// <param name="timeout">超时 缺省:5000 ms</param>
        //public bool SetTagValueToLastValue(int id, byte valueType, object value, int timeout = 5000)
        //{
        //    lock (mlockRealSetObj)
        //    {
        //        CheckLogin();
        //        if (!IsLogin) return false;

        //        try
        //        {
        //            var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 30);
        //            mb.Write(ApiFunConst.SetRealDataToLastData);
        //            mb.Write(this.LoginId);
        //            mb.Write(id);

        //            realSetRequreEvent.Reset();
        //            SendData(mb);

        //            if (realSetRequreEvent.WaitOne(timeout))
        //            {
        //                return mRealSetRequreData.ReadByte() > 0;
        //            }
        //        }
        //        finally
        //        {
        //            mRealSetRequreData?.UnlockAndReturn();
        //            mRealSetRequreData = null;
        //        }
        //        return false;
        //    }
        //}


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
        /// 根据SQL查询历史数据
        /// </summary>
        /// <param name="sql">Sql 语句</param>
        /// <param name="timeout"></param>
        /// <returns>HisQueryTableResult 历史数据 或者List<double> 统计值或者Dictionary<int, TagRealValue> 实时值</returns>
        public object QueryValueBySql(string sql, int timeout = 5000)
        {
            lock (mlockHisQueryObj)
            {
                var vid = 0;
                CheckLogin();
                if (!IsLogin) return null;

                var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + sql.Length + 4);
                mb.Write(ApiFunConst.ReadHisValueBySQL);
                mb.Write(this.LoginId);
                mb.Write(sql);
                mb.Write(vid);
                hisRequreEvent.Reset();

                SendData(mb);
                try
                {
                    if (hisRequreEvent.WaitOne(timeout))
                    {
                        if (mHisRequreData != null && mHisRequreData.WriteIndex - mHisRequreData.ReadIndex > 1)
                        {
                            var vd = mHisRequreData.ReadInt();
                            var byt = mHisRequreData.ReadByte();
                            if (ApiFunConst.ReadHisValueBySQL == byt)
                            {
                                var typ = mHisRequreData.ReadByte();
                                if (typ == 0)
                                {
                                    //历史值
                                    Cdy.Tag.HisQueryTableResult rel = new HisQueryTableResult();
                                    string smeta = mHisRequreData.ReadString();
                                    rel.FromStringToMeta(smeta);
                                    int dsize = mHisRequreData.ReadInt();
                                    rel.Init2(dsize);
                                    mHisRequreData.CopyTo(rel.Address, mHisRequreData.ReadIndex, 0, dsize);
                                    return rel;
                                }
                                else if (typ == 1)
                                {
                                    //统计值
                                    List<double> ltmp = new List<double>();
                                    int dsize = mHisRequreData.ReadInt();
                                    for (int i = 0; i < dsize; i++)
                                    {
                                        ltmp.Add(mHisRequreData.ReadDouble());
                                    }
                                    return ltmp;
                                }
                                else if (typ == 2)
                                {
                                    //实时值
                                    object val = null;
                                    Dictionary<int, TagRealValue> rre = new Dictionary<int, TagRealValue>();
                                    var valuecount = mHisRequreData.ReadInt();
                                    for (int i = 0; i < valuecount; i++)
                                    {
                                        var vvid = mHisRequreData.ReadInt();
                                        var type = mHisRequreData.ReadByte();

                                        switch (type)
                                        {
                                            case (byte)TagType.Bool:
                                                val = ((byte)mHisRequreData.ReadByte());
                                                break;
                                            case (byte)TagType.Byte:
                                                val = ((byte)mHisRequreData.ReadByte());
                                                break;
                                            case (byte)TagType.Short:
                                                val = (mHisRequreData.ReadShort());
                                                break;
                                            case (byte)TagType.UShort:
                                                val = (mHisRequreData.ReadUShort());
                                                break;
                                            case (byte)TagType.Int:
                                                val = (mHisRequreData.ReadInt());
                                                break;
                                            case (byte)TagType.UInt:
                                                val = (mHisRequreData.ReadUInt());
                                                break;
                                            case (byte)TagType.Long:
                                            case (byte)TagType.ULong:
                                                val = (mHisRequreData.ReadULong());
                                                break;
                                            case (byte)TagType.Float:
                                                val = (mHisRequreData.ReadFloat());
                                                break;
                                            case (byte)TagType.Double:
                                                val = (mHisRequreData.ReadDouble());
                                                break;
                                            case (byte)TagType.String:
                                                val = mHisRequreData.ReadString();
                                                break;
                                            case (byte)TagType.DateTime:
                                                val = DateTime.FromBinary(mHisRequreData.ReadLong());
                                                break;
                                            case (byte)TagType.IntPoint:
                                                val = new IntPointData(mHisRequreData.ReadInt(), mHisRequreData.ReadInt());
                                                break;
                                            case (byte)TagType.UIntPoint:
                                                val = new UIntPointData(mHisRequreData.ReadInt(), mHisRequreData.ReadInt());
                                                break;
                                            case (byte)TagType.IntPoint3:
                                                val = new IntPoint3Data(mHisRequreData.ReadInt(), mHisRequreData.ReadInt(), mHisRequreData.ReadInt());
                                                break;
                                            case (byte)TagType.UIntPoint3:
                                                val = new UIntPoint3Data(mHisRequreData.ReadInt(), mHisRequreData.ReadInt(), mHisRequreData.ReadInt());
                                                break;
                                            case (byte)TagType.LongPoint:
                                                val = new LongPointData(mHisRequreData.ReadLong(), mHisRequreData.ReadLong());
                                                break;
                                            case (byte)TagType.ULongPoint:
                                                val = new ULongPointData(mHisRequreData.ReadLong(), mHisRequreData.ReadLong());
                                                break;
                                            case (byte)TagType.LongPoint3:
                                                val = new LongPoint3Data(mHisRequreData.ReadLong(), mHisRequreData.ReadLong(), mHisRequreData.ReadLong());
                                                break;
                                            case (byte)TagType.ULongPoint3:
                                                val = new ULongPoint3Data(mHisRequreData.ReadLong(), mHisRequreData.ReadLong(), mHisRequreData.ReadLong());
                                                break;
                                            case (byte)TagType.Complex:
                                                ComplexRealValue cv = new ComplexRealValue();
                                                int count = mHisRequreData.ReadInt();
                                                for (int j = 0; j < count; j++)
                                                {
                                                    var cvid = mHisRequreData.ReadInt();
                                                    var vtp = mHisRequreData.ReadByte();
                                                    object cval = null;
                                                    switch (vtp)
                                                    {
                                                        case (byte)TagType.Bool:
                                                            cval = ((byte)mHisRequreData.ReadByte());
                                                            break;
                                                        case (byte)TagType.Byte:
                                                            cval = ((byte)mHisRequreData.ReadByte());
                                                            break;
                                                        case (byte)TagType.Short:
                                                            cval = (mHisRequreData.ReadShort());
                                                            break;
                                                        case (byte)TagType.UShort:
                                                            cval = (mHisRequreData.ReadUShort());
                                                            break;
                                                        case (byte)TagType.Int:
                                                            cval = (mHisRequreData.ReadInt());
                                                            break;
                                                        case (byte)TagType.UInt:
                                                            cval = (mHisRequreData.ReadUInt());
                                                            break;
                                                        case (byte)TagType.Long:
                                                        case (byte)TagType.ULong:
                                                            cval = (mHisRequreData.ReadULong());
                                                            break;
                                                        case (byte)TagType.Float:
                                                            cval = (mHisRequreData.ReadFloat());
                                                            break;
                                                        case (byte)TagType.Double:
                                                            cval = (mHisRequreData.ReadDouble());
                                                            break;
                                                        case (byte)TagType.String:
                                                            cval = mHisRequreData.ReadString();
                                                            break;
                                                        case (byte)TagType.DateTime:
                                                            cval = DateTime.FromBinary(mHisRequreData.ReadLong());
                                                            break;
                                                        case (byte)TagType.IntPoint:
                                                            cval = new IntPointData(mHisRequreData.ReadInt(), mHisRequreData.ReadInt());
                                                            break;
                                                        case (byte)TagType.UIntPoint:
                                                            cval = new UIntPointData(mHisRequreData.ReadInt(), mHisRequreData.ReadInt());
                                                            break;
                                                        case (byte)TagType.IntPoint3:
                                                            cval = new IntPoint3Data(mHisRequreData.ReadInt(), mHisRequreData.ReadInt(), mHisRequreData.ReadInt());
                                                            break;
                                                        case (byte)TagType.UIntPoint3:
                                                            cval = new UIntPoint3Data(mHisRequreData.ReadInt(), mHisRequreData.ReadInt(), mHisRequreData.ReadInt());
                                                            break;
                                                        case (byte)TagType.LongPoint:
                                                            cval = new LongPointData(mHisRequreData.ReadLong(), mHisRequreData.ReadLong());
                                                            break;
                                                        case (byte)TagType.ULongPoint:
                                                            cval = new ULongPointData(mHisRequreData.ReadLong(), mHisRequreData.ReadLong());
                                                            break;
                                                        case (byte)TagType.LongPoint3:
                                                            cval = new LongPoint3Data(mHisRequreData.ReadLong(), mHisRequreData.ReadLong(), mHisRequreData.ReadLong());
                                                            break;
                                                        case (byte)TagType.ULongPoint3:
                                                            cval = new ULongPoint3Data(mHisRequreData.ReadLong(), mHisRequreData.ReadLong(), mHisRequreData.ReadLong());
                                                            break;
                                                    }
                                                    var ctime = DateTime.FromBinary(mHisRequreData.ReadLong()).ToLocalTime();
                                                    var cqua = mHisRequreData.ReadByte();
                                                    cv.Add(new ComplexRealValueItem() { Id = cvid, ValueType = vtp, Value = cval, Quality = cqua, Time = ctime });
                                                }
                                                val = cv;
                                                break;
                                        }
                                        var vtime = DateTime.FromBinary(mHisRequreData.ReadLong()).ToLocalTime();
                                        var qua = mHisRequreData.ReadByte();

                                        if (!rre.ContainsKey(vvid))
                                            rre.Add(vvid, new TagRealValue() { Value = val, Quality = qua, Time = vtime });
                                    }
                                    return rre;
                                }
                            }
                        }
                        return null;
                    }

                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData=null;
                }
            }
            return null;
        }

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

                
                try
                {
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                    mb.Write(ApiFunConst.RequestAllHisData);
                    mb.Write(this.LoginId);
                    mb.Write(id);
                    mb.Write(startTime.Ticks);
                    mb.Write(endTime.Ticks);

                    this.hisRequreEvent.Reset();
                    SendData(mb);
                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        return mHisRequreData.Convert<T>();
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
                
                try
                {
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

                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        return mHisRequreData.Convert<T>();
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
        /// 查询某个变量在一系列时间点上的值，忽略系统退出对数据拟合的影响
        /// </summary>
        /// <param name="id">变量Id</param>
        /// <param name="times">时间点集合</param>
        /// <param name="matchType">值拟合类型<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时 缺省:5000 ms</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueAtTimesByIgnorSystemExit<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;

                try
                {
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + times.Count * 8 + 5);
                    mb.Write(ApiFunConst.RequestHisDatasByTimePointIgnorSystemExit);
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

                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        return mHisRequreData.Convert<T>();
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
                
                try
                {
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

                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        return mHisRequreData.Convert<T>();
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
        /// 查询某个变量在一系列时间点上得值,通过指定时间间隔确定时间点，忽略系统退出对数据拟合的影响
        /// </summary>
        /// <param name="id">变量Id</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="span">时间间隔<see cref="System.TimeSpan"/></param>
        /// <param name="matchType">值拟合类型<see cref="Cdy.Tag.QueryValueMatchType"/></param>
        /// <param name="timeout">超时 缺省:5000ms</param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisValueForTimeSpanByIgnorSystemExit<T>(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;

                try
                {
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 24 + 5);
                    mb.Write(ApiFunConst.RequestHisDataByTimeSpanIgnorSystemExit);
                    mb.Write(this.LoginId);
                    mb.Write(id);
                    mb.Write((byte)matchType);
                    mb.Write(startTime.Ticks);
                    mb.Write(endTime.Ticks);
                    mb.Write(span.Ticks);
                    this.hisRequreEvent.Reset();
                    SendData(mb);

                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        return mHisRequreData.Convert<T>();
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
               
                try
                {
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                    mb.Write(ApiFunConst.RequestStatisticData);
                    mb.Write(this.LoginId);
                    mb.Write(id);
                    mb.Write(startTime.Ticks);
                    mb.Write(endTime.Ticks);

                    this.hisRequreEvent.Reset();
                    SendData(mb);

                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        TagType tp = (TagType)mHisRequreData.ReadByte();
                        return ProcessStatisticsResult(mHisRequreData,tp);
                    }
                }
                catch(Exception ex)
                {
                    PrintErroMessage(ex);
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
                
                try
                {
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

                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        TagType tp = (TagType)mHisRequreData.ReadByte();
                        return ProcessStatisticsResult(mHisRequreData, tp);
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
        /// <param name="inteval">偏差</param>
        /// <param name="timeout">超时 缺省:30000</param>
        /// <returns>时间、值</returns>
        public Tuple<DateTime, object> FindTagValue(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value,double inteval, int timeout = 30000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return null;
                
                try
                {
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 256);
                    mb.Write(ApiFunConst.RequestFindTagValue);
                    mb.Write(this.LoginId);
                    mb.Write(id);
                    mb.Write(startTime.Ticks);
                    mb.Write(endTime.Ticks);
                    mb.Write((byte)type);
                    mb.Write(value.ToString());
                    mb.Write(inteval.ToString());

                    this.hisRequreEvent.Reset();
                    SendData(mb);

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
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
        /// <param name="inteval">偏差</param>
        /// <param name="timeout">超时 缺省:30000</param>
        /// <returns>累计时间(单位：秒)</returns>
        public double? CalTagValueKeepTime(int tag, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, double inteval, int timeout=30000)
        {
            lock (mlockHisQueryObj)
            {
                Dictionary<DateTime, object> re = new Dictionary<DateTime, object>();
                CheckLogin();
                
                try
                {
                    if (!IsLogin) return null;
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20+256);
                    mb.Write(ApiFunConst.RequestCalTagValueKeepTime);
                    mb.Write(this.LoginId);
                    mb.Write(tag);
                    mb.Write(startTime.Ticks);
                    mb.Write(endTime.Ticks);
                    mb.Write((byte)type);
                    mb.Write(value.ToString());
                    mb.Write(inteval.ToString());


                    this.hisRequreEvent.Reset();
                    SendData(mb);

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
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
        /// <param name="value">值</param>
        /// <param name="interval">偏差范围</param>
        /// <param name="timeout">超时 缺省:30000</param>
        /// <returns>时间、值对</returns>
        public Dictionary<DateTime, object> FindTagValues(int tag, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, double interval, int timeout=30000)
        {
            lock (mlockHisQueryObj)
            {
                Dictionary<DateTime, object> re = new Dictionary<DateTime, object>();
                CheckLogin();
                if (!IsLogin) return null;
                
                try
                {
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 256);
                    mb.Write(ApiFunConst.RequestFindTagValues);
                    mb.Write(this.LoginId);
                    mb.Write(tag);
                    mb.Write(startTime.Ticks);
                    mb.Write(endTime.Ticks);
                    mb.Write((byte)type);
                    mb.Write(value.ToString());
                    mb.Write(interval.ToString());

                    this.hisRequreEvent.Reset();
                    SendData(mb);

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
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
        /// <param name="timeout">超时 缺省:30000</param>
        /// <returns>最大值和时间列表</returns>
        public Tuple<double, List<DateTime>> FindNumberTagMaxValue(int tag, DateTime startTime, DateTime endTime, int timeout = 30000)
        {
            lock (mlockHisQueryObj)
            {
                List<DateTime> re = new List<DateTime>();
                CheckLogin();
                if (!IsLogin) return null;
                
                try
                {
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                    mb.Write(ApiFunConst.RequestFindNumberTagMaxValue);
                    mb.Write(this.LoginId);
                    mb.Write(tag);
                    mb.Write(startTime.Ticks);
                    mb.Write(endTime.Ticks);

                    this.hisRequreEvent.Reset();
                    SendData(mb);

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
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
        /// <param name="timeout">超时 缺省:30000</param>
        /// <returns>最小值和时间列表</returns>
        public Tuple<double, List<DateTime>> FindNumberTagMinValue(int tag, DateTime startTime, DateTime endTime,int timeout=30000)
        {
            lock (mlockHisQueryObj)
            {
                List<DateTime> re = new List<DateTime>();
                CheckLogin();
                if (!IsLogin) return null;
               
                try
                {
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                    mb.Write(ApiFunConst.RequestFindNumberTagMinValue);
                    mb.Write(this.LoginId);
                    mb.Write(tag);
                    mb.Write(startTime.Ticks);
                    mb.Write(endTime.Ticks);

                    this.hisRequreEvent.Reset();
                    SendData(mb);

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
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
        /// <param name="timeout">超时 缺省:30000</param>
        /// <returns>平局值</returns>
        public double? FindNumberTagAvgValue(int tag, DateTime startTime, DateTime endTime,int timeout=30000)
        {
            lock (mlockHisQueryObj)
            {
                Dictionary<DateTime, object> re = new Dictionary<DateTime, object>();
                CheckLogin();
                if (!IsLogin) return null;
                
                try
                {
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20);
                    mb.Write(ApiFunConst.RequestCalNumberTagAvgValue);
                    mb.Write(this.LoginId);
                    mb.Write(tag);
                    mb.Write(startTime.Ticks);
                    mb.Write(endTime.Ticks);

                    this.hisRequreEvent.Reset();
                    SendData(mb);

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
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
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
        /// 修改历史数据
        /// </summary>
        /// <param name="id">变量ID</param>
        /// <param name="type">变量类型</param>
        /// <param name="user">操作用户</param>
        /// <param name="msg">操作备注</param>
        /// <param name="values">值的集合</param>
        /// <param name="timeout">超时 缺省:30000</param>
        /// <returns></returns>
        public bool ModifyHisData(int id,TagType type,string user,string msg,IEnumerable<TagHisValue<object>> values, int timeout = 30000)
        {
            lock (mlockHisQueryObj)
            {
                CheckLogin();
                if (!IsLogin) return false;
                
                try
                {
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 13 + user.Length * 2 + 2 + msg.Length * 2 + 2 + 5 + values.Count() * 34);
                    mb.Write(ApiFunConst.ModifyHisData);
                    mb.Write(this.LoginId);
                    mb.Write(id);
                    mb.Write(user);
                    mb.Write(msg);
                    mb.Write((byte)type);
                    mb.Write(values.Count());

                    foreach (var vv in values)
                    {
                        mb.Write(vv.Time);
                        switch (type)
                        {
                            case TagType.Bool:
                                mb.Write(Convert.ToByte(Convert.ToBoolean(vv.Value)));
                                break;
                            case TagType.Byte:
                                mb.Write(Convert.ToByte(vv.Value));
                                break;
                            case TagType.UShort:
                                mb.Write(Convert.ToUInt16(vv.Value));
                                break;
                            case TagType.Short:
                                mb.Write(Convert.ToInt16(vv.Value));
                                break;
                            case TagType.Int:
                                mb.Write(Convert.ToInt32(vv.Value));
                                break;
                            case TagType.UInt:
                                mb.Write(Convert.ToUInt32(vv.Value));
                                break;
                            case TagType.Long:
                                mb.Write(Convert.ToInt64(vv.Value));
                                break;
                            case TagType.ULong:
                                mb.Write(Convert.ToUInt64(vv.Value));
                                break;
                            case TagType.Double:
                                mb.Write(Convert.ToDouble(vv.Value));
                                break;
                            case TagType.Float:
                                mb.Write(Convert.ToSingle(vv.Value));
                                break;
                            case TagType.String:
                                mb.Write(Convert.ToString(vv.Value));
                                break;
                            case TagType.DateTime:
                                mb.Write(Convert.ToDateTime(vv.Value).Ticks);
                                break;
                            case TagType.IntPoint:
                                IntPointData ip = (IntPointData)vv.Value;
                                mb.Write(ip.X);
                                mb.Write(ip.Y);
                                break;
                            case TagType.UIntPoint:
                                UIntPointData uip = (UIntPointData)vv.Value;
                                mb.Write(uip.X);
                                mb.Write(uip.Y);
                                break;
                            case TagType.IntPoint3:
                                IntPoint3Data ip3 = (IntPoint3Data)vv.Value;
                                mb.Write(ip3.X);
                                mb.Write(ip3.Y);
                                mb.Write(ip3.Z);
                                break;
                            case TagType.UIntPoint3:
                                UIntPoint3Data uip3 = (UIntPoint3Data)vv.Value;
                                mb.Write(uip3.X);
                                mb.Write(uip3.Y);
                                mb.Write(uip3.Z);
                                break;
                            case TagType.LongPoint:
                                LongPointData lip = (LongPointData)vv.Value;
                                mb.Write(lip.X);
                                mb.Write(lip.Y);
                                break;
                            case TagType.LongPoint3:
                                LongPoint3Data lip3 = (LongPoint3Data)vv.Value;
                                mb.Write(lip3.X);
                                mb.Write(lip3.Y);
                                mb.Write(lip3.Z);
                                break;
                            case TagType.ULongPoint:
                                LongPointData ulip = (LongPointData)vv.Value;
                                mb.Write(ulip.X);
                                mb.Write(ulip.Y);
                                break;
                            case TagType.ULongPoint3:
                                LongPoint3Data ulip3 = (LongPoint3Data)vv.Value;
                                mb.Write(ulip3.X);
                                mb.Write(ulip3.Y);
                                mb.Write(ulip3.Z);
                                break;
                        }
                        mb.WriteByte(vv.Quality);

                    }

                    this.hisRequreEvent.Reset();
                    SendData(mb);

                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        byte cmd = mHisRequreData.ReadByte();
                        if (cmd == ApiFunConst.ModifyHisData)
                        {
                            return mHisRequreData.ReadByte()>0;
                        }
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
                return false;
            }
        }

        /// <summary>
        /// 删除数据
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="user">用户名</param>
        /// <param name="msg">操作备注</param>
        /// <param name="stime">开始时间</param>
        /// <param name="etime">结束时间</param>
        /// <param name="timeout">超时</param>
        /// <returns></returns>
        public bool DeleteHisValue(int id, string user, string msg, DateTime stime, DateTime etime, int timeout = 60000)
        {

            lock (mlockHisQueryObj)
            {
                try
                {
                    CheckLogin();
                    if (!IsLogin) return false;
                    var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 13 + user.Length * 2 + 2 + msg.Length * 2 + 2 + 16 + 4 + 4 + 1);
                    mb.Write(ApiFunConst.DeleteHisData);
                    mb.Write(this.LoginId);
                    mb.Write(id);
                    mb.Write(user);
                    mb.Write(msg);
                    mb.Write(stime);
                    mb.Write(etime);

                    this.hisRequreEvent.Reset();
                    SendData(mb);

                    if (hisRequreEvent.WaitOne(timeout) && mHisRequreData.ReadableCount > 1)
                    {
                        byte cmd = mHisRequreData.ReadByte();
                        if (cmd == ApiFunConst.DeleteHisData)
                        {
                            return mHisRequreData.ReadByte() > 0;
                        }
                    }
                }
                catch (Exception ex)
                {
                    PrintErroMessage(ex);
                }
                finally
                {
                    mHisRequreData?.UnlockAndReturn();
                    mHisRequreData = null;
                }
            }
            return false;
        }
        #endregion

        private void PrintErroMessage(Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class ComplexRealValue : List<ComplexRealValueItem>
    {
    }


    /// <summary>
    /// 
    /// </summary>
    public struct ComplexRealValueItem
    {
        public int Id { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public byte ValueType { get; set; }

        /// <summary>
        /// 值
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// 质量戳
        /// </summary>
        public byte Quality { get; set; }

        /// <summary>
        /// 时间
        /// </summary>
        public DateTime Time { get; set; }

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
            TagType tp = (TagType)data.ReadByte();
            int count = data.ReadInt();
            HisQueryResult<T> re = new HisQueryResult<T>(count);

            data.CopyTo(re.Address, data.ReadIndex, 0, data.WriteIndex - data.ReadIndex);

            re.Count = count;
            data.UnlockAndReturn();
            return re;
        }
    }
}
