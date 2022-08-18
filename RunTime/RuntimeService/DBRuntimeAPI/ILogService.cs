using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBRuntimeAPI
{
    /// <summary>
    /// 
    /// </summary>
    public interface ILogService
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="datebase"></param>
        IEnumerable<string> EnumLogType(string datebase);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="type"></param>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <returns></returns>
        IEnumerable<string> ReadLogs(string database, string type, DateTime starttime, DateTime endtime);
    }
}
