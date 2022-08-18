using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Cdy.Tag;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBGrpcApi
{
    public class TagInfoService : TagInfo.TagInfoBase
    {
        private readonly ILogger<TagInfoService> _logger;
        public TagInfoService(ILogger<TagInfoService> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<TagResultReply> GetTagByName(GetTagByNameRequest request, ServerCallContext context)
        {
            TagResultReply re = new TagResultReply();
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                re.Result = true;

                List<TagBase> tags = new List<TagBase>();
                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                if (tagservice != null)
                {
                    if (request.TagNames != null)
                    {
                        var vtt = tagservice.GetTagsByName(request.TagNames);
                        if (vtt != null)
                        {
                            re.Tags.AddRange(vtt.Select(e => ConvertToLocalTag(e)));
                        }
                    }
                }
                else
                {
                    re.Result = false;
                }
            }
            else
            {
                re.Result = false;
            }
            return Task.FromResult(re);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<TagResultReply> GetTagByGroup(GetTagByGroupRequest request, ServerCallContext context)
        {
            TagResultReply re = new TagResultReply();
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                re.Result = true;

                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                if (tagservice != null)
                {
                    var vtt = tagservice.GetTagsByGroup(request.Group.ToString());
                    if (vtt != null)
                    {
                        re.Tags.AddRange(vtt.Select(e => ConvertToLocalTag(e)));
                    }
                }
                else
                {
                    re.Result = false;
                }
            }
            else
            {
                re.Result = false;
            }
            return Task.FromResult(re);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<ListTagGroupReply> ListTagGroup(ListTagGroupRequest request, ServerCallContext context)
        {
            ListTagGroupReply re = new ListTagGroupReply();
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                if (tagservice != null)
                {
                    re.Group.AddRange(tagservice.ListTagGroups());
                }
                else
                {
                    re.Result = false;
                }
            }
            else
            {
                re.Result = false;
            }
            return Task.FromResult(re);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        private TagBase ConvertToLocalTag(Tagbase tag)
        {
            TagBase re = new TagBase();

            re.Id = tag.Id;
            re.Convert = tag.Conveter != null ? tag.Conveter.SaveToString() : "";
            re.Desc = tag.Desc;
            re.Group = tag.Group;
            re.LinkAddress = tag.LinkAddress;
            re.Name = tag.Name;
            re.ReadWriteType = tag.ReadWriteType.ToString();
            re.Type = tag.Type.ToString();
           
            if (tag is NumberTagBase)
            {
                re.MaxValue = (tag as NumberTagBase).MaxValue.ToString();
                re.MinValue = (tag as NumberTagBase).MinValue.ToString();
            }
            if (tag is FloatingTagBase)
            {
                re.Precision = (tag as FloatingTagBase).Precision.ToString();
            }
            re.SubTags = SaveToXML((tag as ComplexTag).Tags.Values).ToString();
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <returns></returns>
        private XElement SaveToXML(IEnumerable<Tagbase> tags)
        {
            XElement xx = new XElement("Tags");
            foreach(var vv in tags)
            {
                xx.Add(vv.SaveToXML());
            }
            return xx;
        }


    }
}
