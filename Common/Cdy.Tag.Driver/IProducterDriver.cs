using System;
using System.Collections.Generic;

namespace Cdy.Tag.Driver
{

    /// <summary>
    /// 变更
    /// </summary>
    public struct TagChangedArg
    {
        /// <summary>
        /// 
        /// </summary>
        public Dictionary<int,string> AddedTags { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<int,string> ChangedTags { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<int, string> RemoveTags { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct HisTagChangedArg
    {
        public IEnumerable<int> AddedTags { get; set; }

        public IEnumerable<int> ChangedTags { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public IEnumerable<int> RemoveTags { get; set; }
    }



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

        /// <summary>
        /// 编辑类型
        /// </summary>
        string EditType { get; }

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
        /// 
        /// </summary>
        /// <returns></returns>
        bool Pause();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        bool Resume();

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

        /// <summary>
        /// 变量的实时信息改变
        /// </summary>
        /// <param name="arg"></param>
        void OnRealTagChanged(TagChangedArg arg);

        /// <summary>
        /// 变量的历史配置信息改变
        /// </summary>
        /// <param name="arg"></param>
        void OnHisTagChanged(HisTagChangedArg arg);

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
