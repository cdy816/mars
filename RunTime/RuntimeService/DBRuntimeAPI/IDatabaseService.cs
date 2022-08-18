using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBRuntimeAPI
{
    public interface IDatabaseService
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerable<string> ListDatabse();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        bool CheckDatabaseUser(string database,string username, string password);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        bool CheckStart(string database);

        /// <summary>
        /// API 配置
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        Dictionary<string, string> GetDatabaseAPI(string database);

        /// <summary>
        /// 历史数据磁盘占用
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        Tuple<double, double,string> HisDataDisk(string database);

        /// <summary>
        /// 备份路径磁盘占用
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        Tuple<double, double,string> BackHisDataDisk(string database);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        bool RunDatabase(string database);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        bool StopDatabase(string database);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        bool ReStartDatabase(string database);
    }
}
