using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 用于缓存解压数据块
    /// </summary>
    public class DecodeMemoryCachManager
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, MarshalMemoryBlock> mCacheDatas = new Dictionary<string, MarshalMemoryBlock>();

        private string mCacheBasePath;

        /// <summary>
        /// 
        /// </summary>
        public static DecodeMemoryCachManager Manager = new DecodeMemoryCachManager();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="address"></param>
        /// <returns></returns>
        public MarshalMemoryBlock GetMemory(DataFileSeriserbase datafile, long address)
        {
            string skey = System.IO.Path.GetFileNameWithoutExtension(datafile.FileName) + address;
            if(mCacheDatas.ContainsKey(skey))
            {
                return mCacheDatas[skey];
            }
            else
            {
                //string sfile = System.IO.Path.Combine(GetCacheLocation(), skey);
                //if (System.IO.File.Exists(sfile))
                //{
                //    return sfile.LoadFileToMarshalMemory();
                //}
                //else
                //{
                    var re = ReadAndDecompressMemory(datafile, address);
                    mCacheDatas.Add(skey, re);
                    //Task.Run(() => { re.SaveToFile(sfile); });
                    return re;
                //}
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetCacheLocation()
        {
            if(string.IsNullOrEmpty(mCacheBasePath))
            {
                mCacheBasePath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "readcach");
            }
            return mCacheBasePath;
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="address"></param>
        private unsafe MarshalMemoryBlock ReadAndDecompressMemory(DataFileSeriserbase datafile, long address)
        {
            var vsize = datafile.ReadInt(address);
            var dsize = datafile.ReadInt(address + 4);
            var datas = datafile.Read(address + 8, vsize);
            int dtmp = 0;
            MarshalMemoryBlock mmb = new MarshalMemoryBlock(dsize, dsize);

            System.IO.Compression.BrotliDecoder.TryDecompress(new ReadOnlySpan<byte>((void*)datas.Handles[0], vsize), new Span<byte>((void*)mmb.Handles[0], dsize), out dtmp);

            if (dtmp != dsize)
            {
                LoggerService.Service.Warn("DataFileInfo", "解压缩数据长度不一致!" + datafile.FileName);
            }

            datas.Dispose();
           
            return mmb;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
