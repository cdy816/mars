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
using System.Xml.Linq;
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

        public const byte GetTagByFilter = 6;

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
                case GetTagByFilter:
                    loginId = data.ReadLong();
                    if (Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().CheckLogin(loginId))
                    {
                        try
                        {
                            var filter = data.ReadString();
                            Dictionary<string, string> sfilters = new Dictionary<string, string>();
                            if(!string.IsNullOrEmpty(filter))
                            {
                                string[] skeys = filter.Split(new char[] { ';' });
                                foreach(var vv in skeys)
                                {
                                    string[] ss = vv.Split(new char[] { ':' });
                                    if(ss.Length==2 && !sfilters.ContainsKey(ss[0]))
                                    {
                                        sfilters.Add(ss[0], ss[1]);
                                    }
                                }
                            }
                            var vtags = FilterTags(mm.ListAllTags(), sfilters);
                            var totals = vtags.Count();
                            if (sfilters.ContainsKey("Skip"))
                            {
                                var vskip = int.Parse(sfilters["Skip"]);
                                var vtake = int.Parse(sfilters["Take"]);
                                
                                if(vskip+vtake>vtags.Count())
                                {
                                    vtake = totals - vskip;
                                    vtake = vtake < 0 ? 0 : vtake;
                                }
                                vtags = vtags.Skip(vskip).Take(vtake);
                            }

                            List<HisTag> mHisTags = new List<HisTag>();
                            var hserver = Cdy.Tag.ServiceLocator.Locator.Resolve<IHisTagQuery>();
                            if (hserver != null)
                            {
                                foreach (var vv in vtags)
                                {
                                    var htag = hserver.GetHisTagById(vv.Id);
                                    if (htag != null)
                                    {
                                        mHisTags.Add(htag);
                                    }
                                }
                            }

                            System.IO.MemoryStream ms = new System.IO.MemoryStream();
                            System.IO.Compression.GZipStream gs = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionLevel.Optimal);
                            XElement sb = new XElement("Tags");
                            sb.SetAttributeValue("TotalCount", totals);
                            XElement rtag = new XElement("RealTags");
                            foreach (var vv in vtags)
                            {
                                rtag.Add(vv.SaveToXML());
                            }
                            sb.Add(rtag);

                            XElement htags = new XElement("HisTags");
                            foreach(var vv in mHisTags)
                            {
                                htags.Add(vv.SaveToXML());
                            }
                            sb.Add(htags);


                            gs.Write(Encoding.Unicode.GetBytes(sb.ToString()));
                            gs.Flush();

                            var re = Parent.Allocate(ApiFunConst.TagInfoRequest, (int)ms.Position + 4);
                            re.Write((int)ms.Position);
                            re.Write(ms.GetBuffer(), 0, (int)ms.Position);

                            gs.Close();
                            ms.Close();

                            Parent.AsyncCallback(client, re);
                        }
                        catch(Exception ex)
                        {
                            LoggerService.Service.Warn("TagInfoServerProcess", ex.Message);
                        }
                    }
                    break;
                case Hart:
                    loginId = data.ReadLong();
                    Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().FreshUserId(loginId);
                    break;
                case Login:
                    string user = data.ReadString();
                    string pass = data.ReadString();
                    long result = Cdy.Tag.ServiceLocator.Locator.Resolve<IRuntimeSecurity>().Login(user, pass,client);
                    Parent.AsyncCallback(client, ToByteBuffer(ApiFunConst.TagInfoRequest, result));
                    //Debug.Print("处理登录并返回:"+result);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="res"></param>
        /// <param name="vfilters"></param>
        /// <returns></returns>
        private IEnumerable<Tagbase> FilterTags(IEnumerable<Tagbase> res, Dictionary<string,string> vfilters)
        {
            var hisval = Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IHisTagQuery>();
            if (vfilters.Count == 0) return res;

            return res.Where((tag) => {
                var re = true;
                foreach (var vv in vfilters)
                {
                    switch (vv.Key)
                    {
                        case "keyword":
                            bool btmp = false;
                            string[] ss = vv.Value.Split(" ", StringSplitOptions.RemoveEmptyEntries);
                            foreach (var vvv in ss)
                            {
                                btmp |= (tag.Name.Contains(vvv) || tag.Desc.Contains(vvv));
                            }
                            re = re && btmp;
                            break;
                        case "group":
                            re = re && (tag.Group == vv.Value);
                            break;
                        case "type":
                            re = re && ((int)tag.Type == int.Parse(vv.Value));
                            break;
                        case "readwritetype":
                            re = re && ((int)tag.ReadWriteType == int.Parse(vv.Value));
                            break;
                        case "recordtype":
                            int ival = int.Parse(vv.Value);
                            if (ival == 3)
                            {
                                re = true;
                            }
                            else if(hisval!=null)
                            {
                                var histag = hisval.GetHisTagById(tag.Id);
                                if(histag!=null)
                                {
                                    re = re && ((int)histag.Type == ival);
                                }
                                else
                                {
                                    re = false;
                                }
                            }
                            break;
                        case "compresstype":
                            ival = int.Parse(vv.Value);
                            if (ival == -1)
                            {
                                re = re && hisval.GetHisTagById(tag.Id)!=null;
                            }
                            else if(hisval != null)
                            {
                                var histag = hisval.GetHisTagById(tag.Id);

                                if (histag!=null)
                                {
                                    re = re && ((int)histag.CompressType == ival);
                                }
                                else
                                {
                                    re = false;
                                }
                            }
                            break;
                        case "linkaddress":
                            re = re && (tag.LinkAddress.Contains(vv.Value));
                            break;


                    }

                }

                return re;

            });
        }
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
