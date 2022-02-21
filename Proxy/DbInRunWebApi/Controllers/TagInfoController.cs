using Cdy.Tag;
using DbInRunWebApi.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NSwag.Annotations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DbWebApi.Controllers
{
    /// <summary>
    /// 变量
    /// </summary>
    [Route("[controller]")]
    [ApiController]
    [OpenApiTag("变量配置信息服务", Description = "变量配置信息服务")]
    public class TagInfoController : ControllerBase
    {
        /// <summary>
        /// 获取变量配置信息
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet()]
        public TagInfoResponse Get([FromBody] TagInfoRequest request)
        {
            TagInfoResponse re = new TagInfoResponse();
            if (DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                re.Result = true;

                List<object> tags = new List<object>();
                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                if (tagservice != null)
                {
                    if (request.TagNames != null)
                    {
                        var vtt = tagservice.GetTagsByName(request.TagNames);
                        if (vtt != null)
                        {
                            tags.AddRange(vtt.Select(e=> ConvertToLocalTag(e)));
                        }
                    }
                    re.Tags = tags;
                }
                else
                {
                    re.Result = false;
                    re.ErroMessage = "tag service not ready";
                }
            }
            else
            {
                re.Result = false;
                re.ErroMessage = "not login";
            }
            return re;
        }

        /// <summary>
        /// 获取某个变量组下所有变量
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("GetByGroup")]
        public TagInfoResponse GetByGroup([FromBody] TagInfoGroupRequest request)
        {
            TagInfoResponse re = new TagInfoResponse();
            if (DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                re.Result = true;

                List<object> tags = new List<object>();
                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                if (tagservice != null)
                {
                    var vtt = tagservice.GetTagsByGroup(request.Group);
                    if (vtt != null)
                    {
                        tags.AddRange(vtt.Select(e=>ConvertToLocalTag(e)));
                    }
                    re.Tags = tags;
                }
                else
                {
                    re.Result = false;
                    re.ErroMessage = "tag service not ready";
                }
            }
            else
            {
                re.Result = false;
                re.ErroMessage = "not login";
            }
            return re;
        }

        /// <summary>
        /// 枚举变量组的集合
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpGet("ListTagGroup")]
        public TagInfoGroupResponse ListTagGroup([FromBody] Requestbase request)
        {
            TagInfoGroupResponse re = new TagInfoGroupResponse();
            if (DbInRunWebApi.SecurityManager.Manager.IsLogin(request.Token))
            {
                var tagservice = ServiceLocator.Locator.Resolve<ITagManager>();
                if (tagservice != null)
                {
                    re.Groups = tagservice.ListTagGroups();
                }
                else
                {
                    re.Result = false;
                    re.ErroMessage = "tag service not ready";
                }
            }
            else
            {
                re.Result = false;
                re.ErroMessage = "not login";
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        private object ConvertToLocalTag(Tagbase tag)
        {
            if(tag is FloatingTagBase)
            {
                return new LocalFloatTag().CloneFrom(tag);
            }
            else if(tag is NumberTagBase)
            {
                return new LocalNumberTag().CloneFrom(tag);
            }
            else
            {
                return new LocalTagBase().CloneFrom(tag);
            }
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class LocalTagBase
    {
        /// <summary>
        /// 编号
        /// </summary>
        public int Id { get; set; } = -1;

        /// <summary>
        /// 类型
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; } = "";

        /// <summary>
        /// 组
        /// </summary>
        public string Group { get; set; } 

        /// <summary>
        /// 描述
        /// </summary>
        public string Desc { get; set; } = "";

        /// <summary>
        /// 外部管理IO的地址
        /// </summary>
        public string LinkAddress { get; set; } = "";

        /// <summary>
        /// 值转换函数
        /// </summary>
        public string Conveter { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string ReadWriteType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public virtual LocalTagBase CloneFrom(Cdy.Tag.Tagbase tag)
        {
            this.Conveter = tag.Conveter != null ? tag.Conveter.SaveToString() : "";
            this.Desc = tag.Desc;
            this.Group = tag.Group;
            this.Id = tag.Id;
            this.LinkAddress = tag.LinkAddress;
            this.Name = tag.Name;
            this.ReadWriteType = tag.ReadWriteType.ToString();
            this.Type = tag.Type.ToString();
            return this;
        }

    }

    /// <summary>
    /// 
    /// </summary>
    public class LocalNumberTag: LocalTagBase
    {
        /// <summary>
        /// 最大值
        /// </summary>
        public double MaxValue { get; set; }

        /// <summary>
        /// 最小值
        /// </summary>
        public double MinValue { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public override LocalTagBase CloneFrom(Tagbase tag)
        {
            base.CloneFrom(tag);
            NumberTagBase ntag = tag as NumberTagBase;
            this.MaxValue = ntag.MaxValue;
            this.MinValue = ntag.MinValue;
            return this;
        }

    }

    /// <summary>
    /// 
    /// </summary>
    public class LocalFloatTag:LocalNumberTag
    {
        /// <summary>
        /// 小数位数
        /// </summary>
        public byte Precision { get; set; } = 2;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public override LocalTagBase CloneFrom(Tagbase tag)
        {
            base.CloneFrom(tag);
           FloatingTagBase ntag = tag as FloatingTagBase;
            this.Precision = ntag.Precision;
            return this;
        }
    }

}
