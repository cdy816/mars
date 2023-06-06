using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DirectAccessMqtt
{

    public class MessageBase
    {
        /// <summary>
        /// 功能函数
        /// </summary>
        public string Fun { get; set; }
    }

    public class LoginRequest: MessageBase
    {
        public LoginRequest()
        {
            Fun = "0";
        }
        /// <summary>
        /// 用户名
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// 密码
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// 设备名称，
        /// </summary>
        public string DeviceId { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class LoginResponse: MessageBase
    {
        /// <summary>
        /// 
        /// </summary>
        public LoginResponse()
        {
            Fun = "0";
        }
        /// <summary>
        /// 
        /// </summary>
        public string Token { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class CommonReponseBase : MessageBase
    {
        public string CallId { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class BoolReponse:CommonReponseBase
    {
        public BoolReponse()
        {
            Fun = "2";
        }
        public bool Result { get; set; } 
    }


    /// <summary>
    /// 上传消息
    /// </summary>
    public class UpdateMessageRequest : MessageBase
    {

        /// <summary>
        /// Token
        /// </summary>
        public string Token { get; set; }

        /// <summary>
        /// 调用ID
        /// </summary>
        public string CallId { get; set; }
    }

    /// <summary>
    /// 上传设备值
    /// </summary>
    public class UpdateDeviceValueRequest: UpdateMessageRequest
    {
        public UpdateDeviceValueRequest()
        {
            Fun = "1";
        }
        /// <summary>
        /// 
        /// </summary>
        public RealValue[] Values { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class UpdateAreaDeviceValueRequest : UpdateMessageRequest
    {
        public UpdateAreaDeviceValueRequest()
        {
            Fun = "2";
        }
        public RealValue[] Values { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class RegistorCallBackRequest : UpdateMessageRequest
    {
        public RegistorCallBackRequest()
        {
            Fun = "4";
        }
        public string[] Tags { get; set; }
    }

    /// <summary>
    /// 实时值
    /// </summary>
    public class RealValue
    {
        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 值
        /// </summary>
        public object Value { get; set; }
    }

    /// <summary>
    /// 更新历史值
    /// </summary>
    public class UpdateHisValueRequest:UpdateMessageRequest
    {
        public UpdateHisValueRequest()
        {
            Fun = "3";
        }
        /// <summary>
        /// 
        /// </summary>
        public HisValue[] Values { get; set; }
    }

    /// <summary>
    /// 历史值
    /// </summary>
    public class HisValue
    {
        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public HisValueItem[] Values { get; set; }
    }

    /// <summary>
    /// 实时值
    /// </summary>
    public class HisValueItem
    {
        /// <summary>
        /// 时间
        /// </summary>
        public DateTime Time { get; set; }
        /// <summary>
        /// 值
        /// </summary>
        public object Value { get; set; }
    }



    /// <summary>
    /// 
    /// </summary>
    public class DeviceValueSetRequest : MessageBase
    {

        public DeviceValueSetRequest()
        {
            Fun = "1";
        }

        /// <summary>
        /// 
        /// </summary>
        public string CallId { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string Tag { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Value { get; set; }
    }

    /// <summary>
    /// 设置下发回调
    /// </summary>
    public class DeviceSetResponse : MessageBase
    {
        /// <summary>
        /// 
        /// </summary>
        public DeviceSetResponse()
        {
            Fun = "5";
        }
        /// <summary>
        /// 调用ID
        /// </summary>
        public string CallId { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string DeviceTopic { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Tag { get; set; }


        /// <summary>
        /// 下发结果
        /// </summary>
        public bool Result { get; set; }
    }

}
