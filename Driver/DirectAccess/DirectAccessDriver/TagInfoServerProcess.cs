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
using System.Drawing;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Cdy.Tag;
using Cdy.Tag.Driver;
using Cheetah;

namespace DirectAccessDriver
{
    public class TagInfoServerProcess : ServerProcessBase
    {

        #region ... Variables  ...
        
        public const byte GetTagIdByNameFun = 0;

        public const byte QueryAllTagNameAndIds = 2;

        public const byte GetTagIdByFilterRegistor = 3;

        public const byte GetDriverRecordTypeTagIds = 5;

        public const byte GetDriverRecordTypeTagIds2 = 51;

        public const byte GetDatabaseName = 4;

        public const byte Login = 1;
        /// <summary>
        /// 
        /// </summary>
        public const byte Login2 = 11;

        private Dictionary<string,int[]> mClients = new Dictionary<string, int[]>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        public override byte FunId => APIConst.TagInfoRequestFun;

        /// <summary>
        /// 
        /// </summary>
        public override bool IsEnableMutiThread => false;

        #endregion ...Properties...

        #region ... Methods    ...



        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        public unsafe override void ProcessSingleData(string client, ByteBuffer data)
        {
            var mm = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.ITagManager>();
            byte sfun = data.ReadByte();
            switch (sfun)
            {
                case GetTagIdByNameFun:
                    long loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId) || loginId < 1)
                    {
                        int count = data.ReadInt();
                        if (count > 0)
                        {
                            var re = Parent.Allocate(APIConst.TagInfoRequestFun, (count + 1) * 4 + 1);
                            re.Write(GetTagIdByNameFun);
                            re.Write(count);

                            //首先删除旧的缓冲
                            //Span<int> sp=null;
                            int[] sp;
                            lock (mClients)
                            {

                                sp = new int[count];
                                if (mClients.ContainsKey(client))
                                {
                                    mClients[client] = sp;
                                }
                                else
                                {
                                    mClients.Add(client, sp);
                                }
                            }

                            for (int i = 0; i < count; i++)
                            {
                                // string stag = data.ReadString();
                                var ival = mm.GetTagIdByName(data.ReadString());
                                if (ival.HasValue)
                                {
                                    re.Write(ival.Value);
                                    sp[i] = ival.Value;
                                }
                                else
                                {
                                    re.Write((int)-1);
                                    sp[i] = -1;
                                }
                            }

                            Parent.AsyncCallback(client, re);
                        }
                    }
                    break;
                case QueryAllTagNameAndIds:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        int psize = 500000;
                        var vtags = mm.ListAllTags().Where(e => e.LinkAddress.StartsWith("DirectAccess") || e.Type == TagType.Complex);
                        int tcount = vtags.Count() / psize;
                        tcount += (vtags.Count() % psize > 0 ? 1 : 0);
                        for (int i = 0; i < tcount; i++)
                        {
                            if ((i + 1) * psize > vtags.Count())
                            {
                                var vv = vtags.Skip(i * psize).Take(vtags.Count() % psize);
                                Parent.AsyncCallback(client, GetTagBuffer(vv, (short)i, (short)tcount));
                            }
                            else
                            {
                                var vv = vtags.Skip(i * psize).Take(psize);
                                Parent.AsyncCallback(client, GetTagBuffer(vv, (short)i, (short)tcount));
                            }
                        }

                        if (tcount == 0)
                        {
                            Parent.AsyncCallback(client, GetTagBuffer(new List<Tagbase>(), (short)0, (short)0));
                        }

                    }
                    break;
                case GetTagIdByFilterRegistor:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        string filter = data.ReadString();
                        int psize = 500000;
                        var vtags = mm.ListAllTags().Where(e => e.LinkAddress.StartsWith("DirectAccess") && e.LinkAddress.Contains(filter));
                        int tcount = vtags.Count() / psize;
                        tcount += (vtags.Count() % psize > 0 ? 1 : 0);
                        for (int i = 0; i < tcount; i++)
                        {
                            if ((i + 1) * psize > vtags.Count())
                            {
                                var vv = vtags.Skip(i * psize).Take(vtags.Count() % psize);
                                Parent.AsyncCallback(client, GetTagBuffer(vv, (short)i, (short)tcount));
                            }
                            else
                            {
                                var vv = vtags.Skip(i * psize).Take(psize);
                                Parent.AsyncCallback(client, GetTagBuffer(vv, (short)i, (short)tcount));
                            }
                        }

                        if (tcount == 0)
                        {
                            Parent.AsyncCallback(client, GetTagBuffer(new List<Tagbase>(), (short)0, (short)0));
                        }

                    }
                    break;
                case GetDriverRecordTypeTagIds:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        int psize = 500000;

                        var vserver = ServiceLocator.Locator.Resolve<IHisTagQuery>();

                        var vtags = vserver.ListAllDriverRecordTags();
                        int tcount = vtags.Count() / psize;
                        tcount += (vtags.Count() % psize > 0 ? 1 : 0);
                        for (int i = 0; i < tcount; i++)
                        {
                            if ((i + 1) * psize > vtags.Count())
                            {
                                var vv = vtags.Skip(i * psize).Take(vtags.Count() % psize);
                                Parent.AsyncCallback(client, GetRecordTypeBuffer(vv, (short)i, (short)tcount));
                            }
                            else
                            {
                                var vv = vtags.Skip(i * psize).Take(psize);
                                Parent.AsyncCallback(client, GetRecordTypeBuffer(vv, (short)i, (short)tcount));
                            }
                        }

                        if (tcount == 0)
                        {
                            Parent.AsyncCallback(client, GetRecordTypeBuffer(new List<HisTag>(), (short)0, (short)0));
                        }
                    }
                    break;
                case GetDriverRecordTypeTagIds2:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {

                        var vserver = ServiceLocator.Locator.Resolve<IHisTagQuery>();
                        int icount = data.ReadInt();

                        ByteBuffer re = Parent.Allocate(APIConst.TagInfoRequestFun, icount + 5);
                        re.WriteByte(GetDriverRecordTypeTagIds2);
                        re.Write(icount);
                        for (int i = 0; i < icount; i++)
                        {
                            if (vserver.GetHisTagById(data.ReadInt())?.Type == RecordType.Driver)
                            {
                                re.WriteByte(1);
                            }
                            else
                            {
                                re.WriteByte(0);
                            }
                        }
                        Parent.AsyncCallback(client, re);
                    }
                    break;
                case GetDatabaseName:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        string dbname = ServiceLocator.Locator.Resolve<ITagManager>().Name;
                        var re = Parent.Allocate(APIConst.TagInfoRequestFun, dbname.Length + 4);
                        re.Write(GetDatabaseName);
                        re.Write(dbname);
                        Parent.AsyncCallback(client, re);
                    }
                    break;
                case Login:
                    try
                    {
                        string user = data.ReadString();
                        string pass = data.ReadString();
                        long result = Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().Login(user, pass, client);

                        if (result > 0)
                        {
                            lock (mClients)
                            {
                                if (!mClients.ContainsKey(client))
                                    mClients.Add(client, null);
                            }
                            LoggerService.Service.Info("DirectAccessDriver", user + " at client " + client + " login sucessfull " + result);
                        }
                        else
                        {
                            LoggerService.Service.Warn("DirectAccessDriver", user + " at client " + client + " login failed.");
                        }

                        Parent.AsyncCallback(client, ToByteBuffer(APIConst.TagInfoRequestFun, Login, result));
                    }
                    catch (Exception eex)
                    {
                        LoggerService.Service.Erro("DirectAccessDriver", $"{eex.Message}:{eex.StackTrace}");
                    }
                    break;
                case Login2:
                    try
                    {
                        loginId = data.ReadLong();
                        if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                        {
                            string user = data.ReadString();
                            string pass = data.ReadString();
                            string result = Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().Login(user, pass);
                            Parent.AsyncCallback(client, ToByteBuffer(APIConst.TagInfoRequestFun, Login2, result));
                        }
                    }
                    catch (Exception eex)
                    {
                        LoggerService.Service.Erro("DirectAccessDriver", $"{eex.Message}:{eex.StackTrace}");
                    }
                    break;
            }
        }

        /// <summary>
        /// 通知数据发送变化
        /// </summary>
        public void NotifyDatabaseChanged(bool realchanged, bool hischanged)
        {
            byte val = 0;
            if (realchanged) val += 1;
            if (hischanged) val += 2;
           
            if (val > 0)
            {
                ByteBuffer data = ToByteBuffer(APIConst.DatabaseChangedNotify, val);
                foreach (var vv in mClients)
                {
                    Parent.AsyncCallback(vv.Key, data);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="bcount"></param>
        /// <param name="totalcount"></param>
        /// <returns></returns>
        private ByteBuffer GetTagBuffer(IEnumerable<Tagbase> tags,short bcount,short totalcount)
        {
            ByteBuffer re = Parent.Allocate(APIConst.TagInfoRequestFun, tags.Count() * 302+9);
            re.Write(QueryAllTagNameAndIds);
            re.Write(totalcount);
            re.Write(bcount);
            re.Write(tags.Count());
            foreach(var vv in tags)
            {
                re.Write(vv.Id);
                re.Write(vv.FullName);
                re.WriteByte((byte)vv.Type);
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="bcount"></param>
        /// <param name="totalcount"></param>
        /// <returns></returns>
        private ByteBuffer GetRecordTypeBuffer(IEnumerable<HisTag> tags, short bcount, short totalcount)
        {
            ByteBuffer re = Parent.Allocate(APIConst.TagInfoRequestFun, tags.Count() * 4 + 9);
            re.Write(GetDriverRecordTypeTagIds);
            re.Write(totalcount);
            re.Write(bcount);
            re.Write(tags.Count());
            foreach (var vv in tags)
            {
                re.Write(vv.Id);
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public unsafe override void OnClientDisconnected(string id)
        {
            if (mClients.ContainsKey(id))
            {
                //Tuple<IntPtr,int> pp = mClients[id];
                var pp = mClients[id];

                lock (mClients)
                    mClients.Remove(id);
                
                //to do update bad quality
                if(pp!=null)
                {
                    var service = ServiceLocator.Locator.Resolve<IRealTagProduct>();
                    //Span<int> sp = new Span<int>((byte*)pp.Item1, pp.Item2*4);
                    DateTime dtime = DateTime.Now;
                    //for(int i=0;i<pp.Item2;i++)
                    for(int i=0;i<pp.Length;i++)
                    {
                        int ip = pp[i];
                        if (ip > -1)
                        {
                            service.SetTagQuality(ip, (byte)QualityConst.Offline, dtime);
                        }
                    }

                    //Marshal.FreeHGlobal(pp.Item1);

                }
            }
            base.OnClientDisconnected(id);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
