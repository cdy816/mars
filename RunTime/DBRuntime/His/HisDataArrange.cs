//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by chongdaoyang at 2021/1/27 12:54:55.
//  Version 1.0
//  CHONGDAOYANGPC
//==============================================================
using Cdy.Tag;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 对历史数据文件格式内容进行重新整理,将一个文件内的同一个变量的数据合并在一起,提高数据查询效率
    /// </summary>
    public class HisDataArrange
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        public static HisDataArrange Arrange = new HisDataArrange();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 拷贝文件头部
        /// </summary>
        private void CopyFileHead(System.IO.Stream msource,System.IO.Stream mtarget)
        {
            byte[] bval = ArrayPool<byte>.Shared.Rent(DataFileManager.FileHeadSize);
            msource.Read(bval, 0, DataFileManager.FileHeadSize);
            mtarget.Write(bval, 0, DataFileManager.FileHeadSize);
            ArrayPool<byte>.Shared.Return(bval);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msource"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        private int ReadInt(System.IO.Stream msource,long offset)
        {
            int re = 0;
            byte[] bval = ArrayPool<byte>.Shared.Rent(4);
            msource.Position = offset;
            msource.Read(bval, 0, 4);
            re = BitConverter.ToInt32(bval);
            ArrayPool<byte>.Shared.Return(bval);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msource"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        private long ReadLong(System.IO.Stream msource, long offset)
        {
            long re = 0;
            byte[] bval = ArrayPool<byte>.Shared.Rent(8);
            msource.Position = offset;
            var size = msource.Read(bval, 0, 8);
            if (size == 8)
            {
                re = BitConverter.ToInt64(bval);
            }
            else
            {
                re = -1;
            }
            ArrayPool<byte>.Shared.Return(bval);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msource"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        private unsafe MarshalFixedMemoryBlock Read(System.IO.Stream msource, long offset,int size)
        {
            msource.Position = offset;
            MarshalFixedMemoryBlock re = new MarshalFixedMemoryBlock(size);
            msource.Read(new Span<byte>((void*)re.StartMemory, size));
            return re;
        }

        /// <summary>
        /// 拷贝数据区
        /// </summary>
        /// <param name="msource"></param>
        /// <param name="offset"></param>
        /// <param name="mtarget"></param>
        private void CopyDataRegion(System.IO.Stream msource,long sourceOffset,System.IO.Stream mtarget)
        {
            msource.Position = sourceOffset;

            int fileDuration = 0;
            int blockDuration = 0;
            int tagcount = 0;
            

            //copy data region head
            byte[] bval = ArrayPool<byte>.Shared.Rent(48);
            msource.Read(bval, 0, 48);
            mtarget.Write(bval, 0, 48);

            tagcount = BitConverter.ToInt32(bval, 24);
            fileDuration = BitConverter.ToInt32(bval, 36);
            blockDuration = BitConverter.ToInt32(bval, 40);

            int blockcount = fileDuration * 60 / blockDuration;

            //copy data ids
            msource.Read(bval, 0, 4);
            int size = BitConverter.ToInt32(bval);
            mtarget.Write(bval, 0, 4);

            if (size > 0)
            {
                var bval2 = ArrayPool<byte>.Shared.Rent(size);
                msource.Read(bval2, 0, size);
                mtarget.Write(bval2, 0, size);
                ArrayPool<byte>.Shared.Return(bval2);
            }
            ArrayPool<byte>.Shared.Return(bval);

            long pheadpointlocation = mtarget.Position;

            var sourceheadpoint = Read(msource, msource.Position, tagcount * 12 * blockcount);

            MarshalFixedMemoryBlock targetheadpoint;

            CopyRegionData(msource, sourceheadpoint,tagcount,blockcount, mtarget,out targetheadpoint);
            long lp = mtarget.Position;
            //write tag data pointer
            mtarget.Position = pheadpointlocation;
            targetheadpoint.WriteToStream(mtarget);

            mtarget.Position = lp;

            targetheadpoint.Dispose();
            sourceheadpoint.Dispose();
        }

        /// <summary>
        /// 拷贝DataRegion数据
        /// </summary>
        /// <param name="msource">源流</param>
        /// <param name="sourceheadpoint">源数据块指针</param>
        /// <param name="tagcount">变量个数</param>
        /// <param name="blockcount">数据块个数</param>
        /// <param name="mtarget">目标流</param>
        /// <param name="targetheadpoint">目标数据块</param>
        private unsafe void CopyRegionData(System.IO.Stream msource, MarshalFixedMemoryBlock sourceheadpoint, int tagcount, int blockcount, System.IO.Stream mtarget, out MarshalFixedMemoryBlock targetheadpoint)
        {
            MarshalFixedMemoryBlock re = new MarshalFixedMemoryBlock(sourceheadpoint.Length);
            MarshalFixedMemoryBlock data = new MarshalFixedMemoryBlock(blockcount * 3300 * 2);
            long mtargetposition = mtarget.Position;
            int offset = 0;
            for (int i = 0; i < tagcount; i++)
            {
                data.Clear();
                offset = 0;
                mtargetposition = mtarget.Position;
                for (int j = 0; j < blockcount; j++)
                {
                    var dataoffset = sourceheadpoint.ReadInt(j * tagcount * 12 + i * 12);
                    var baseaddress = sourceheadpoint.ReadLong(j * tagcount * 12 + i * 12+4);

                    //重新组织数据块指针的分布形式，使得一个变量的数据块指针在一起
                    re.WriteInt(j  * 12 + i * blockcount * 12, offset);
                    re.WriteLong(j * 12 + i * blockcount * 12 + 4, mtargetposition);

                    if (dataoffset < 0)
                    {
                        baseaddress = baseaddress + (dataoffset & 0x7FFFFFFF);
                    }
                    else
                    {
                        baseaddress = baseaddress + (dataoffset);
                    }

                    var datasize = ReadInt(msource,baseaddress);

                    data.WriteInt(offset, datasize);
                    offset += 4;
                    msource.Read(new Span<byte>((void*)(data.Buffers + offset), datasize));
                    offset += datasize;
                }

                data.WriteToStream(mtarget, 0, offset);
            }

            data.Dispose();

            targetheadpoint = re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="msource"></param>
        /// <returns></returns>
        private List<long> ListDataAreas(System.IO.Stream msource)
        {
            List<long> re = new List<long>();
            msource.Position = DataFileManager.FileHeadSize;
            re.Add(msource.Position);
            long nextp = ReadLong(msource, msource.Position + 8);
            while( nextp>0)
            {
                re.Add(nextp);
                nextp = ReadLong(msource,nextp + 8);
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceFile"></param>
        /// <param name="targetFile"></param>
        public bool ReArrange(string sourceFile,string targetFile)
        {
            try
            {
                var source = GetDataFileSerise(sourceFile);
                var target = GetDataFileSerise(targetFile);

                var sstream = source.GetStream();
                var tstream = target.GetStream();

                CopyFileHead(sstream, tstream);

                var dataarealoc = ListDataAreas(sstream);

                long lastarealoc = -1;

                foreach (var vv in dataarealoc)
                {
                    CopyDataRegion(sstream, vv, tstream);
                    if (lastarealoc > 0)
                    {
                        //更新下个指针数据区
                        var lp = tstream.Position;
                        target.Write(lp, lastarealoc + 8);
                        //更新上一个区域的指针
                        target.Write(lastarealoc, lp);
                        lastarealoc = lp;
                    }
                }
                source.Dispose();
                target.Dispose();
            }
            catch
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        private DataFileSeriserbase GetDataFileSerise(string file)
        {
            var re = DataFileSeriserManager.manager.GetDefaultFileSersie().New();
            re.CreatOrOpenFile(file);
            return re;
        }

        /// <summary>
        /// 检查并转化历史文件内容格式
        /// </summary>
        /// <param name="file"></param>
        /// <param name="fileDuration"></param>
        /// <param name="removeold"></param>
        public void CheckAndReArrangeHisFile(string file,int fileDuration=4,bool removeold=true)
        {
            try
            {
                if (System.IO.File.Exists(file))
                {
                    System.IO.FileInfo finfo = new System.IO.FileInfo(file);
                    //如果文件的创建时间，到现在超过一个文件能够保存的最大时间
                    if ((DateTime.Now - finfo.CreationTime).TotalHours > (fileDuration + 0.5))
                    {
                        string tdirect = System.IO.Path.GetDirectoryName(file);
                        string targetfile = System.IO.Path.Combine(tdirect, System.IO.Path.GetFileNameWithoutExtension(file) + ".his");

                        if (ReArrange(file, targetfile) && removeold)
                        {
                            System.IO.File.Delete(file);
                        }
                    }
                }
            }
            catch
            {

            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
