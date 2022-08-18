using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public interface IDataCompressService
    {
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startime"></param>
        /// <param name="datas"></param>
        /// <param name="qualitys"></param>
        /// <returns></returns>
        MarshalMemoryBlock CompressData<T>(int id, DateTime startime, SortedDictionary<DateTime, T> datas,SortedDictionary<DateTime,byte> qualitys);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startime"></param>
        /// <param name="datas"></param>
        /// <returns></returns>
        MarshalMemoryBlock CompressData<T>(int id, DateTime startime, HisQueryResult<T> datas);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        void Release(MarshalMemoryBlock data);
    }
}
