using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Cdy.Tag.Common
{
    /// <summary>
    /// 
    /// </summary>
    public class IdBuffer : IDisposable
    {

        #region ... Variables  ...

        private IntPtr[] mBuffers;
        public const int bufferSize = 1024 * 1024 * 8 * 8;
        private object mLocker = new object();
        private bool mIsDisposed = false;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public IdBuffer()
        {
            mBuffers = ArrayPool<IntPtr>.Shared.Rent(1024);
            mBuffers.AsSpan().Clear();
        }

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        private void ReSizeBuffer(int size)
        {
            var newbuffer = ArrayPool<IntPtr>.Shared.Rent(size);
            newbuffer.AsSpan().Clear();
            mBuffers.CopyTo(newbuffer, 0);
            ArrayPool<IntPtr>.Shared.Return(mBuffers);
            mBuffers = newbuffer;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public unsafe void SetId(int id)
        {
            int idd = id / bufferSize;
            if(idd>mBuffers.Length)
            {
                ReSizeBuffer(idd);
            }

            if (mBuffers[idd] == IntPtr.Zero)
            {
                mBuffers[idd] = Marshal.AllocHGlobal(bufferSize/8);
                int dindex = id % bufferSize;
                byte bval = (byte)(0x01 << (dindex % 8));
                MemoryHelper.WriteByte((void*)mBuffers[idd], dindex / 8, bval);
            }
            else
            {
                int dindex = id % bufferSize;
                byte bval = MemoryHelper.ReadByte((void*)mBuffers[idd], dindex / 8);
                bval = (byte)(bval | (byte)(0x01 << (dindex % 8)));
                MemoryHelper.WriteByte((void*)mBuffers[idd], dindex / 8, bval);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public unsafe void ClearId(int id)
        {
            int idd = id / bufferSize;
            if (idd > mBuffers.Length)
            {
                ReSizeBuffer(idd);
            }

            if (mBuffers[idd] != IntPtr.Zero)
            {
                int dindex = id % bufferSize;
                byte bval = MemoryHelper.ReadByte((void*)mBuffers[idd], dindex / 8);
                bval = (byte)(bval ^ (byte)(0x01 << (dindex % 8)));
                MemoryHelper.WriteByte((void*)mBuffers[idd], dindex / 8, bval);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public unsafe bool CheckId(int id)
        {
            int idd = id / bufferSize;
            if (idd > mBuffers.Length || mBuffers[idd] == IntPtr.Zero) return false;
            int dindex = id % bufferSize;
            byte bval = MemoryHelper.ReadByte((void*)mBuffers[idd], dindex / 8);
            return (bval & (1 << (dindex % 8))) > 0;
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void Dispose()
        {
            lock (mLocker)
            {
                if (mIsDisposed) return;
                mIsDisposed = true;
                try
                {
                    if (mBuffers != null)
                    {
                        foreach (var vv in mBuffers)
                        {
                            try
                            {
                                if (vv != IntPtr.Zero)
                                {
                                    Marshal.FreeHGlobal(vv);
                                }
                            }
                            catch
                            {

                            }

                        }
                        ArrayPool<IntPtr>.Shared.Return(mBuffers);
                    }
                }
                catch(Exception ex)
                {
                    LoggerService.Service.Erro("IdBuffer", ex.Message);
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
