//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:00:38.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Cdy.Tag;
using DotNetty.Buffers;

namespace SpiderDriver
{
    public class TagInfoServerProcess : ServerProcessBase
    {

        #region ... Variables  ...
        
        public const byte GetTagIdByNameFun = 0;

        public const byte QueryAllTagNameAndIds = 2;

        public const byte GetDriverRecordTypeTagIds = 5;

        public const byte GetDriverRecordTypeTagIds2 = 51;

        public const byte Login = 1;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        public override byte FunId => APIConst.TagInfoRequestFun;
                
        #endregion ...Properties...

        #region ... Methods    ...

       

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected unsafe override void ProcessSingleData(string client, IByteBuffer data)
        {
            var mm = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.ITagManager>();
            byte sfun = data.ReadByte();
            switch (sfun)
            {
                case GetTagIdByNameFun:
                    long loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        int count = data.ReadInt();
                        if (count > 0)
                        {
                            var re = BufferManager.Manager.Allocate(APIConst.TagInfoRequestFun, (count+1) * 4);
                            re.WriteInt(count);
                            for (int i = 0; i < count; i++)
                            {
                                string stag = data.ReadString();
                                var ival = mm.GetTagIdByName(stag);
                                if (ival.HasValue)
                                {
                                    re.WriteInt(ival.Value);
                                }
                                else
                                {
                                    re.WriteInt((int)-1);
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
                        int psize = 100000;
                        var vtags = mm.ListAllTags();
                        int tcount = vtags.Count() / psize;
                        tcount += (vtags.Count() % psize > 0 ? 1 : 0);
                        for(int i=0;i<tcount;i++)
                        {
                            if((i+1)*psize>vtags.Count())
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
                    }
                    break;
                case GetDriverRecordTypeTagIds:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        int psize = 100000;

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
                    }
                    break;
                case GetDriverRecordTypeTagIds2:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                    
                        var vserver = ServiceLocator.Locator.Resolve<IHisTagQuery>();
                        int icount = data.ReadInt();

                        IByteBuffer re = BufferManager.Manager.Allocate(APIConst.TagInfoRequestFun, icount + 5);
                        re.WriteByte(GetDriverRecordTypeTagIds2);
                        re.WriteInt(icount);
                        for (int i = 0; i < icount; i++)
                        {
                            if(vserver.GetHisTagById(data.ReadInt()).Type == RecordType.Driver)
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
                case Login:
                    string user = data.ReadString();
                    string pass = data.ReadString();
                    long result = Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().Login(user, pass, client);
                    Parent.AsyncCallback(client, ToByteBuffer(APIConst.TagInfoRequestFun, result));
                    break;

            }


        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="bcount"></param>
        /// <param name="totalcount"></param>
        /// <returns></returns>
        private IByteBuffer GetTagBuffer(IEnumerable<Tagbase> tags,short bcount,short totalcount)
        {
            IByteBuffer re = BufferManager.Manager.Allocate(APIConst.TagInfoRequestFun, tags.Count() * 64+9);
            re.WriteByte(QueryAllTagNameAndIds);
            re.WriteShort(totalcount);
            re.WriteShort(bcount);
            re.WriteInt(tags.Count());
            foreach(var vv in tags)
            {
                re.WriteInt(vv.Id);
                re.WriteString(vv.FullName);
                re.WriteByte((byte)vv.Type);
            }
            return re;
        }

        private IByteBuffer GetRecordTypeBuffer(IEnumerable<HisTag> tags, short bcount, short totalcount)
        {
            IByteBuffer re = BufferManager.Manager.Allocate(APIConst.TagInfoRequestFun, tags.Count() * 4 + 9);
            re.WriteByte(GetDriverRecordTypeTagIds);
            re.WriteShort(totalcount);
            re.WriteShort(bcount);
            re.WriteInt(tags.Count());
            foreach (var vv in tags)
            {
                re.WriteInt(vv.Id);
            }
            return re;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
