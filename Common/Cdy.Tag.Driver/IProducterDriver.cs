using System;
using System.Collections.Generic;

namespace Cdy.Tag.Driver
{
    /// <summary>
    /// 
    /// </summary>
    public interface IProducterDriver
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 名称,唯一不可冲突
        /// </summary>
        string Name { get; }

        /// <summary>
        /// 支持的寄存器集合
        /// </summary>
        string[] Registors { get; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 初始化
        /// </summary>
        /// <returns></returns>
        bool Init();

        /// <summary>
        /// 开启
        /// </summary>
        /// <returns></returns>
        bool Start(IRealTagProduct tagQuery,ITagHisValueProduct tagHisValueService);

        /// <summary>
        /// 停止
        /// </summary>
        /// <returns></returns>
        bool Stop();

        /// <summary>
        /// 获取某个数据库参数配置
        /// </summary>
        /// <param name="database">数据库名称</param>
        /// <returns></returns>
        Dictionary<string,string> GetConfig(string database);

        /// <summary>
        /// 更新针对某个数据库的配置
        /// </summary>
        /// <param name="database">数据库名称</param>
        /// <param name="config">配置</param>
        void UpdateConfig(string database, Dictionary<string, string> config);

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
