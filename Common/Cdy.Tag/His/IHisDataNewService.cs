using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{

    /// <summary>
    /// 
    /// </summary>
    public enum SaveType
    {
        /// <summary>
        /// 追加
        /// </summary>
        Append,
        /// <summary>
        /// 替换
        /// </summary>
        Replace
    }


    public interface IHisDataManagerService
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        string GetHisFileName(int id, DateTime time);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="data"></param>
        /// <param name="saveType"></param>
        /// <param name="filename"></param>
        /// <returns></returns>
        bool SaveData(int id,DateTime time, MarshalMemoryBlock data, SaveType saveType,string filename);


        /// <summary>
        /// 清空历史文件
        /// </summary>
        /// <returns></returns>
        bool ClearTmpFile();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        void Take(string file);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        void Release(string file);
    }
}
