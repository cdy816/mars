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
using System.Diagnostics;
using System.Text;
using System.Threading;
using Cdy.Tag;
using Cheetah;
using System.Linq;
//using DotNetty.Buffers;

namespace DBHighApi.Api
{
    public class TagInfoServerProcess : ServerProcessBase
    {

        #region ... Variables  ...
        
        //
        public const byte GetTagIdByNameFun = 0;

        //
        public const byte ListAllTagFun = 2;

        public const byte ListTagGroup = 3;

        public const byte GetTagByGroup = 4;

        public const byte Login = 1;

        public const byte Hart = 5;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        public override byte FunId => ApiFunConst.TagInfoRequest;
                
        #endregion ...Properties...

        #region ... Methods    ...

       

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        protected unsafe override void ProcessSingleData(string client, ByteBuffer data)
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
                            var re = Parent.Allocate(ApiFunConst.TagInfoRequest,count * 4);
                            for(int i=0;i<count;i++)
                            {
                                var ival = mm.GetTagIdByName(data.ReadString());
                                if (ival.HasValue)
                                {
                                    re.Write(ival.Value);
                                }
                                else
                                {
                                    re.Write((int)-1);
                                }
                            }
                            Parent.AsyncCallback(client, re);
                        }
                    }
                    break;
                case ListAllTagFun:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        try
                        {
                            var tags = mm.ListAllTags().ToDictionary(e => e.Id, e => e.FullName);

                            System.IO.MemoryStream ms = new System.IO.MemoryStream();
                            System.IO.Compression.GZipStream gs = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionLevel.Optimal);
                            StringBuilder sb = new StringBuilder();
                            foreach (var vv in tags)
                            {
                                sb.Append(vv.Key + "," + vv.Value + ";");
                            }
                            gs.Write(Encoding.Unicode.GetBytes(sb.ToString()));
                            gs.Flush();

                            var re = Parent.Allocate(ApiFunConst.TagInfoRequest, (int)ms.Position + 4);
                            re.Write((int)ms.Position);

                            re.Write(ms.GetBuffer(),0,(int)ms.Position);

                            gs.Close();
                            ms.Close();

                            Parent.AsyncCallback(client, re);
                        }
                        catch
                        {

                        }
                    }
                    break;
                case ListTagGroup:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        try
                        {
                            var tags = mm.ListTagGroups();
                            
                            System.IO.MemoryStream ms = new System.IO.MemoryStream();
                            System.IO.Compression.GZipStream gs = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionLevel.Optimal);
                            StringBuilder sb = new StringBuilder();
                            foreach (var vv in tags)
                            {
                                sb.Append(vv+ ",");
                            }
                            sb.Length = sb.Length > 1 ? sb.Length - 1 : sb.Length;
                            
                            gs.Write(Encoding.Unicode.GetBytes(sb.ToString()));
                            gs.Flush();
                            var re = Parent.Allocate(ApiFunConst.TagInfoRequest, (int)ms.Position + 4);
                            re.Write((int)ms.Position);

                            re.Write(ms.GetBuffer(), 0, (int)ms.Position);

                            gs.Close();
                            ms.Close();

                            Parent.AsyncCallback(client, re);
                        }
                        catch
                        {

                        }
                    }
                    break;
                case GetTagByGroup:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        try
                        {
                            var tags = mm.GetTagsByGroup(data.ReadString());
                            var vtags = tags != null ? tags.ToDictionary(e => e.Id, e => e.FullName):new Dictionary<int, string>();
                            System.IO.MemoryStream ms = new System.IO.MemoryStream();
                            System.IO.Compression.GZipStream gs = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionLevel.Optimal);
                            StringBuilder sb = new StringBuilder();
                            foreach (var vv in vtags)
                            {
                                sb.Append(vv.Key + "," + vv.Value + ";");
                            }
                            gs.Write(Encoding.Unicode.GetBytes(sb.ToString()));
                            gs.Flush();

                            var re = Parent.Allocate(ApiFunConst.TagInfoRequest, (int)ms.Position + 4);
                            re.Write((int)ms.Position);
                            re.Write(ms.GetBuffer(), 0, (int)ms.Position);

                            gs.Close();
                            ms.Close();

                            Parent.AsyncCallback(client, re);
                        }
                        catch
                        {

                        }
                    }
                    break;
                case Hart:
                    loginId = data.ReadLong();
                    
                    break;
                case Login:
                    string user = data.ReadString();
                    string pass = data.ReadString();
                    long result = Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().Login(user, pass,client);
                    Parent.AsyncCallback(client, ToByteBuffer(ApiFunConst.TagInfoRequest, result));
                    Debug.Print("处理登录并返回:"+result);
                    break;
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
