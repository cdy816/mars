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

            if(System.IO.File.Exists(filename))
            {
                mStream = System.IO.File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                return false;
            }
            else
            {
                mStream = System.IO.File.Create(filename, 1024,FileOptions.WriteThrough);
                return true;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public override void Flush()
        {
            mStream.Flush();
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Close()
        {
            if (mStream != null)
            {
                mStream.Close();
            }
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
        public override MemoryBlock Read(long start, int len)
        {
            MemoryBlock re = new MemoryBlock(len);
            mStream.Position = start;
            mStream.Write(re.Memory, 0, len);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="start"></param>
        /// <param name="len"></param>
        public override void Write(byte[] source, long start)
        {
            mStream.Position = start;
            mStream.Write(source, 0, source.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="start"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public override void Write(byte[] source, long start, int offset, int len)
        {
            mStream.Position = start;
            mStream.Write(source, offset, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public override void Append(byte[] source, int offset, int len)
        {
            mStream.Position = mStream.Length;
            mStream.Write(source, offset, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public override long ReadLong(long start)
        {
            mStream.Position = start;
            byte[] re = new byte[8];
            mStream.Read(re, 0, re.Length);
            return BitConverter.ToInt64(re, 0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public override int ReadInit(long start)
        {
            mStream.Position = start;
            byte[] re = new byte[4];
            mStream.Read(re, 0, re.Length);
            return BitConverter.ToInt32(re, 0);
        }

        public override DateTime ReadDateTime(long start)
        {
            mStream.Position = start;
            byte[] re = new byte[8];
            mStream.Read(re, 0, re.Length);
            return DateTime.FromBinary(BitConverter.ToInt64(re, 0));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <returns></returns>
        public override short ReadShort(long start)
        {
            mStream.Position = start;
            byte[] re = new byte[2];
            mStream.Read(re, 0, re.Length);
            return BitConverter.ToInt16(re, 0);
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
        public override void Write(DateTime value, long start)
        {
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public override void Write(long value, long start)
        {
            mStream.Position = start;
            var re = BitConverter.GetBytes(value);
            mStream.Write(re, 0, re.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public override void Write(int value, long start)
        {
            mStream.Position = start;
            var re = BitConverter.GetBytes(value);
            mStream.Write(re, 0, re.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public override void Write(short value, long start)
        {
            mStream.Position = start;
            var re = BitConverter.GetBytes(value);
            mStream.Write(re, 0, re.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="start"></param>
        public override void Write(byte value, long start)
        {
            mStream.Position = start;
            mStream.Write(new byte[] { value }, 0, 1);
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
        /// <param name="filename"></param>
        /// <returns></returns>
        public override bool OpenFile(string filename)
        {
            if (System.IO.File.Exists(filename))
            {
                mStream = System.IO.File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                return true;
            }
            return false;
        }
    }
}
