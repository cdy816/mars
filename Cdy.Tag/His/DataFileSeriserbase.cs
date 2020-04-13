//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class DataFileSeriserbase:IDisposable
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        
        /// <summary>
        /// 
        /// </summary>
        public abstract string Name { get; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public abstract DataFileSeriserbase Close();



        /// <summary>
        /// 创建新的画面
        /// </summary>
        public abstract bool CreatOrOpenFile(string filename);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename"></param>
        /// <returns></returns>
        public abstract bool OpenFile(string filename);

        /// <summary>
        /// 检查是否存在
        /// </summary>
        /// <param name="filename"></param>
        /// <returns></returns>
        public abstract bool CheckExist(string filename);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public abstract DataFileSeriserbase Write(DateTime value, long start);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public abstract DataFileSeriserbase Write(long value, long start);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public abstract DataFileSeriserbase Write(int value, long start);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public abstract DataFileSeriserbase Write(short value, long start);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public abstract DataFileSeriserbase Write(byte value, long start);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="start"></param>
        /// <param name="len"></param>
        public abstract DataFileSeriserbase Write(byte[] source, long start);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="start"></param>
        public abstract DataFileSeriserbase Write(List<byte[]> source, long start);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="start"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public abstract DataFileSeriserbase Write(List<byte[]> source, long start,int offset,int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public abstract DataFileSeriserbase Append(byte[] source, int offset, int len);

        /// <summary>
        /// 附加空值
        /// </summary>
        /// <param name="len"></param>
        public abstract DataFileSeriserbase AppendZore(int len);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public abstract DataFileSeriserbase Append(List<byte[]> source, int offset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="start"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public abstract DataFileSeriserbase Write(byte[] source, long start,int offset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <param name="len"></param>
        public abstract MarshalMemoryBlock Read(long start, int len);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public abstract long ReadLong(long start);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public abstract DateTime ReadDateTime(long start);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public abstract int ReadInit(long start);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public abstract short ReadShort(long start);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public abstract byte ReadByte(long start);

        /// <summary>
        /// 
        /// </summary>
        public abstract long CurrentPostion { get; }


        /// <summary>
        /// 
        /// </summary>
        public abstract long Length { get; }

        /// <summary>
        /// 
        /// </summary>
        public abstract DataFileSeriserbase GoToEnd();

        /// <summary>
        /// 刷新数据
        /// </summary>
        public abstract DataFileSeriserbase Flush();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public abstract DataFileSeriserbase New();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public abstract Stream GetStream();

        /// <summary>
        /// 
        /// </summary>
        public virtual void Dispose()
        {
            throw new NotImplementedException();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
