using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace DBRuntimeServer
{
    public class LogServerService : LogServer.LogServerBase
    {
        private readonly ILogger<LogServerService> _logger;
        public LogServerService(ILogger<LogServerService> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetLogResponse> GetLog(GetLogRequest request, ServerCallContext context)
        {

            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new GetLogResponse() { Result = false });

            GetLogResponse re = new GetLogResponse() { Result = true };
            var ls = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.ILogService>();
            var logs = ls.ReadLogs(request.Database, request.LogType, DateTime.Parse(request.StartTime), DateTime.Now);
            var vmsg = ZipFile(logs);
            re.Msg = Google.Protobuf.ByteString.CopyFrom(vmsg);
            return Task.FromResult(re);
        }

        /// <summary>
        /// Ñ¹ËõÎÄ¼þ
        /// </summary>
        /// <param name="sfile"></param>
        private byte[] ZipFile(IEnumerable<string> ls)
        {
            try
            {
                using (System.IO.MemoryStream ms = new MemoryStream())
                {
                    using (System.IO.Compression.BrotliStream bs = new System.IO.Compression.BrotliStream(ms, System.IO.Compression.CompressionLevel.Optimal))
                    {
                        using (var sw = new System.IO.StreamWriter(bs))
                        {
                            foreach (var vv in ls)
                            {
                                sw.WriteLine(vv);
                            }
                        }
                    }
                    return ms.ToArray();
                }
               
            }
            catch
            {
                return new byte[0];
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetLogResponse> GetLog2(GetLogRequest2 request, ServerCallContext context)
        {
            if (!UserManager.Manager.CheckLogin(request.Token)) return Task.FromResult(new GetLogResponse() { Result = false } );

            GetLogResponse re = new GetLogResponse() { Result = true };
            var ls = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.ILogService>();
            var logs = ls.ReadLogs(request.Database, request.LogType, DateTime.Parse(request.StartTime), DateTime.Parse(request.EndTime));
            var vmsg = ZipFile(logs);
            re.Msg = Google.Protobuf.ByteString.CopyFrom(vmsg);
            //re.Msg.Add(vmsg);
            return Task.FromResult(re);
        }

    }
}
