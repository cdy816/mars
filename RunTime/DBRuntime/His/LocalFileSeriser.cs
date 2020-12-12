//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class LocalFileSeriser : DataFileSeriserbase
    {

        #region ... Variables  ...

        private System.IO.Stream mStream;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public override string Name => "LocalFile";

        #endregion ...Properties...

        #region ... Methods    ...


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

        /// <summary>
        /// 
        /// </summary>
        public override long CurrentPostion => mStream.Position;

        /// <summary>
        /// 
        /// </summary>
        public override long Length => mStream.Length;


        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename"></param>
        public override bool CreatOrOpenFile(string filename)
        {
            this.FileName = filename;
            if(System.IO.File.Exists(filename))
            {
                mStream = System.IO.File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                return false;
            }
            else
            {
                string dir = System.IO.Path.GetDirectoryName(filename);
                if (!string.IsNullOrEmpty(dir) && !System.IO.Directory.Exists(dir))
                {
                    System.IO.Directory.CreateDirectory(dir);
                }
                mStream = System.IO.File.Create(filename, 1024,FileOptions.WriteThrough);
                return true;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public override DataFileSeriserbase Flush()
        {
            try
            {
                if (mStream != null && mStream.CanWrite)
                    mStream.Flush();
            }
            catch
            {

            }
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        public override DataFileSeriserbase Close()
        {
            try
            {
                if (mStream != null)
                {
                    mStream.Close();
                }
            }
            catch
            {

            }
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename"></param>
        /// <returns></returns>
        public override bool CheckExist(string filename)
        {
            return System.IO.File.Exists(filename);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public override MarshalMemoryBlock Read(long start, int len)
        {
            //var vtmp = len / 1024 * 100;
            //vtmp = len % (1024 * 100) > 0 ? vtmp + 1 : vtmp;

            MarshalMemoryBlock re = new MarshalMemoryBlock(len, 1024 * 100);
            mStream.Position = start;

            //byte[] bval = new byte[len];
            //mStream.Read(bval, 0, len);
            //re.WriteBytesDirect(0, bval);

            re.ReadFromStream(mStream, len);

            //mStream.Write(re.StartMemory, 0, len);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="start"></param>
        /// <param name="len"></param>
        public override DataFileSeriserbase Write(byte[] source, long start)
        {
            mStream.Position = start;
            mStream.Write(source, 0, source.Length);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="start"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public override DataFileSeriserbase Write(byte[] source, long start, int offset, int len)
        {
            mStream.Position = start;
            mStream.Write(source, offset, len);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public override DataFileSeriserbase Append(byte[] source, int offset, int len)
        {
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            mStream.Position = mStream.Length;
            mStream.Write(source, offset, len);
            //LoggerService.Service.Info("LocalFileSeriser", "写入文件耗时:" + sw.ElapsedMilliseconds + " 文件大小:" + len / 1024.0 / 1024.0);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="len"></param>
        public override DataFileSeriserbase AppendZore(int len)
        {
            mStream.Position = mStream.Length;
            int size = 1024 * 128;
            //byte[] bvals = new byte[size];

            var bvals = ArrayPool<byte>.Shared.Rent(size);

            //var bvals = ArrayPool<byte>.Shared.Rent(size);
            Array.Clear(bvals, 0, bvals.Length);
            try
            {
                for (int i = 0; i < (len / size); i++)
                {
                    mStream.Write(bvals, 0, size);
                }
                int csize = len % size;
                if (csize > 0)
                {
                    mStream.Write(bvals, 0, csize);
                }
            }
            catch
            {

            }
            ArrayPool<byte>.Shared.Return(bvals);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public override long ReadLong(long start)
        {
            mStream.Position = start;
            //byte[] re = new byte[8];
            var re = ArrayPool<byte>.Shared.Rent(8);
            
            try
            {
                mStream.Read(re, 0, 8);
                return BitConverter.ToInt64(re, 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public override int ReadInt(long start)
        {
            mStream.Position = start;
            //byte[] re = new byte[4];
            var re = ArrayPool<byte>.Shared.Rent(4);
            
            try
            {
                mStream.Read(re, 0, 4);
                return BitConverter.ToInt32(re, 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public override float ReadFloat(long start)
        {
            mStream.Position = start;
            // byte[] re = new byte[4];
            var re = ArrayPool<byte>.Shared.Rent(4);
            try
            {
                mStream.Read(re, 0, 4);
                return MemoryHelper.ReadFloat(re);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public override double ReadDouble(long start)
        {
            mStream.Position = start;
            // byte[] re = new byte[8];
            var re = ArrayPool<byte>.Shared.Rent(8);
            try
            {
                mStream.Read(re, 0, 8);
                return MemoryHelper.ReadDouble(re);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public override byte[] ReadBytes(long start, int len)
        {
            mStream.Position = start;
            byte[] re = new byte[len];
            mStream.Read(re, 0, re.Length);
            return re;
        }

        public override DateTime ReadDateTime(long start)
        {
            mStream.Position = start;
            //byte[] re = new byte[8];
            var re = ArrayPool<byte>.Shared.Rent(8);
            try
            {
                mStream.Read(re, 0, 8);

                return MemoryHelper.ReadDateTime(re);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(re);
            }
            // return DateTime.FromBinary(BitConverter.ToInt64(re, 0));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public override short ReadShort(long start)
        {
            mStream.Position = start;
            //byte[] re = new byte[2];
            var re = ArrayPool<byte>.Shared.Rent(2);
            try
            {
                mStream.Read(re, 0, 2);
                return BitConverter.ToInt16(re, 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(re);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public override byte ReadByte(long start)
        {
            mStream.Position = start;
            return (byte)mStream.ReadByte();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public override DataFileSeriserbase Write(DateTime value, long start)
        {
            mStream.Position = start;
            //mStream.Write(MemoryHelper.GetBytes(value), 0, 8);
            // Write(value.ToBinary(), start);

            byte[] re = ArrayPool<byte>.Shared.Rent(8);
            Unsafe.As<byte, DateTime>(ref re[0]) = value;
            //var re = BitConverter.GetBytes(value);
            mStream.Write(re, 0, 8);
            ArrayPool<byte>.Shared.Return(re);

            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public override DataFileSeriserbase Write(long value, long start)
        {
            mStream.Position = start;

            byte[] re = ArrayPool<byte>.Shared.Rent(8);
            Unsafe.As<byte, long>(ref re[0]) = value;
            //var re = BitConverter.GetBytes(value);
            mStream.Write(re, 0, 8);
            ArrayPool<byte>.Shared.Return(re);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public override DataFileSeriserbase Write(int value, long start)
        {
            mStream.Position = start;
            byte[] re = ArrayPool<byte>.Shared.Rent(4);
            Unsafe.As<byte, long>(ref re[0]) = value;
            //var re = BitConverter.GetBytes(value);
            mStream.Write(re, 0, 4);
            ArrayPool<byte>.Shared.Return(re);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public override DataFileSeriserbase Write(short value, long start)
        {
            mStream.Position = start;
            byte[] re = ArrayPool<byte>.Shared.Rent(2);
            Unsafe.As<byte, long>(ref re[0]) = value;
            //var re = BitConverter.GetBytes(value);
            mStream.Write(re, 0, 2);
            ArrayPool<byte>.Shared.Return(re);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public override DataFileSeriserbase Write(byte value, long start)
        {
            mStream.Position = start;
            mStream.WriteByte(value);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override DataFileSeriserbase New()
        {
            return new LocalFileSeriser();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override Stream GetStream()
        {
            return mStream;
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Dispose()
        {
            Close();
            if(mStream!=null)
            {
                mStream = null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CheckAndOpen()
        {
            if(mStream==null || !mStream.CanRead)
            return OpenFile(FileName);
            return true;
        }

        public override DataFileSeriserbase CloseAndReOpen()
        {
            long pos = mStream.Position;
            Close();
            OpenFile(FileName);
            mStream.Position = pos;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename"></param>
        /// <returns></returns>
        public override bool OpenFile(string filename)
        {
            if (System.IO.File.Exists(filename))
            {
                this.FileName = filename;
                mStream = System.IO.File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                return true;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename"></param>
        /// <returns></returns>
        public override bool OpenForReadOnly(string filename)
        {
            if (System.IO.File.Exists(filename))
            {
                this.FileName = filename;
                mStream = System.IO.File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                return true;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        public override DataFileSeriserbase GoToEnd()
        {
            mStream.Position = mStream.Length;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="start"></param>
        public override DataFileSeriserbase Write(List<byte[]> source, long start, int offset, int len)
        {
            List<Tuple<int, int, int>> copyIndex = new List<Tuple<int, int, int>>();

            int sstart = offset;
            int count = 0;
            int precount = 0;
            int copyed = 0;
            for (int i = 0; i < source.Count; i++)
            {
                precount = count;
                count = source[i].Length + count;
                if (count > sstart)
                {
                    var ltmp = count - sstart;
                    if (ltmp > (len - copyed))
                    {
                        copyIndex.Add(new Tuple<int, int, int>(i, sstart - precount, len - copyed));
                        break;
                    }
                    else
                    {
                        copyIndex.Add(new Tuple<int, int, int>(i, sstart - precount, ltmp));
                        sstart += ltmp;
                        copyed += ltmp;
                    }
                }
            }
            mStream.Position = start;
            foreach (var vv in copyIndex)
            {
                mStream.Write(source[vv.Item1], vv.Item2, vv.Item3);
            }
            return this;
        }


        public override DataFileSeriserbase Write(List<byte[]> source, long start)
        {
            mStream.Position = start;
            for (int i = 0; i < source.Count; i++)
            {
                mStream.Write(source[i], 0, source[i].Length);
            }
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public override DataFileSeriserbase Append(List<byte[]> source, int offset, int len)
        {
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            List<Tuple<int, int, int>> copyIndex = new List<Tuple<int, int, int>>();

            mStream.Position = mStream.Length;

            int start = offset;
            int count = 0;
            int precount = 0;
            int copyed = 0;
            for(int i=0;i<source.Count;i++)
            {
                precount = count;
                count = source[i].Length + count;
                if (count > start)
                {
                    var ltmp = count - start;
                    if (ltmp > (len - copyed))
                    {
                        copyIndex.Add(new Tuple<int, int, int>(i, start - precount, len - copyed));
                        break;
                    }
                    else
                    {
                        copyIndex.Add(new Tuple<int, int, int>(i, start - precount, ltmp));
                        start += ltmp;
                        copyed += ltmp;
                    }
                }
            }

            foreach(var vv in copyIndex)
            {
                mStream.Write(source[vv.Item1], vv.Item2, vv.Item3);
            }
            //sw.Stop();
            //LoggerService.Service.Info("LocalFileSeriser", "写入文件耗时:" + sw.ElapsedMilliseconds + " 文件大小:" + len / 1024.0 / 1024.0);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool IsOpened()
        {
            return mStream != null;
        }
    }
}
