using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 历史数据修正
    /// 补丁文件和历史文件名称严格匹配，也是以4小时、10万个变量为一个文件
    /// 文件结构：
    /// HisPacthFileHead+[HisPatchDataRegion]
    /// HisPacthFileHead:version(4)+timeduration(4)(单位分钟)+tagcountperfile(4)+IdEnableArea(100000/8)
    /// HisPatchDataRegion:Id(4)+DataSize(4)+[ValueDataArea]+UserDataLen(2)+UserData+MsgLen(2)+Msg
    /// ValueDataArea:ValueType(1)+TimeArea+ValueArea+QualityArea
    /// </summary>
    public class HisDataPatchSeriser:IHisDataPatch
    {
        /// <summary>
        /// 
        /// </summary>
        public static HisDataPatchSeriser patchSeriser = new HisDataPatchSeriser();

        HisDataPatchMemory mMemory;

        private string mCurentFile=null;

        private int mFileHeadCount = 0;

        private bool mIsBusy = false;

        private object mLockObj = new object();

        public float Version { get; set; } = 0.1f;

        /// <summary>
        /// 
        /// </summary>
        public int FileDuration { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string DatabaseName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string DataSeriser { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DataFileSeriserbase FileWriter { get; set; }

        /// <summary>
        /// 数据文件扩展名
        /// </summary>
        public const string DataFileExtends = ".hisp";

        /// <summary>
        /// 单个文件内变量的个数
        /// </summary>
        public int TagCountOneFile { get; set; } = 100000;

        /// <summary>
        /// 
        /// </summary>
        private void CheckFileWriter()
        {
            if(FileWriter == null)
            {
                FileWriter = DataFileSeriserManager.manager.GetSeriser(DataSeriser).New();
                mFileHeadCount = TagCountOneFile / 8 + 12;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetDataPath(int id,DateTime time)
        {
            return System.IO.Path.Combine(SeriseEnginer5.HisDataPath, GetFileName(id,time));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetFileName(int Id,DateTime time)
        {
            var vid = Id / TagCountOneFile;
            return  DatabaseName + vid.ToString("X3") + time.ToString("yyyyMMdd") + FileDuration.ToString("D2") + (time.Hour / FileDuration).ToString("D2") + DataFileExtends;
        }

        /// <summary>
        /// 
        /// </summary>
        private void AppendFileHead()
        {
            FileWriter.Write(BitConverter.GetBytes(Version),0);
            FileWriter.Write(FileDuration, 4);
            FileWriter.Write(TagCountOneFile,8);
            FileWriter.AppendZore(TagCountOneFile / 8);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        private void CheckFile(int id, DateTime time)
        {
            var vfile = GetDataPath(id, time);
            if (vfile != mCurentFile)
            {
                if (FileWriter != null)
                {
                    FileWriter.Flush();
                    FileWriter.Close();
                }
                CheckFileWriter();
                vfile = mCurentFile;

                if (FileWriter.CreatOrOpenFile(vfile))
                {
                    AppendFileHead();
                }
                else if(FileWriter.Length< mFileHeadCount)
                {
                    AppendFileHead();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="valuecount"></param>
        /// <returns></returns>
        private int CalValueSize(int valuecount, TagType type)
        {
            switch (type)
            {
                case TagType.Bool:
                case TagType.Byte:
                    return (8 + 1 + 1) * valuecount;
                case TagType.Short:
                case TagType.UShort:
                    return (8 + 2 + 1) * valuecount;
                case TagType.Int:
                case TagType.UInt:
                case TagType.Float:
                    return (8 + 4 + 1) * valuecount;
                case TagType.Long:
                case TagType.ULong:
                case TagType.Double:
                case TagType.DateTime:
                    return (8 + 8 + 1) * valuecount;
                case TagType.String:
                    return (8 + 256 + 1) * valuecount;
                case TagType.IntPoint:
                case TagType.UIntPoint:
                    return (8 + 8 + 1) * valuecount;
                case TagType.IntPoint3:
                case TagType.UIntPoint3:
                    return (8 + 12 + 1) * valuecount;
                case TagType.LongPoint:
                case TagType.ULongPoint:
                    return (8 + 16 + 1) * valuecount;
                case TagType.LongPoint3:
                case TagType.ULongPoint3:
                    return (8 + 24 + 1) * valuecount;

            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Take()
        {
            lock(mLockObj)
            {
                while(mIsBusy)
                {
                    Thread.Sleep(10);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="user"></param>
        /// <param name="msg"></param>
        /// <param name="valuecount"></param>
        public void BeginPatch(int id,TagType type,string user,string msg,int valuecount)
        {
            mIsBusy = true;

            var vsize = CalValueSize(valuecount,type);
            if (mMemory == null)
            {
                mMemory = new HisDataPatchMemory(vsize) { User = user,Msg=msg,Id=id,ValueCount=valuecount,ValueType = (byte)type,TimeIndex=-1};
            }
            else
            {
                mMemory.ValueType = (byte)type;
                mMemory.CheckAndResize(vsize);
                mMemory.TimeIndex = -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public void AppendPatchValue(DateTime time,object value,byte quality)
        {
            int tid = time.Hour / this.FileDuration;
            if(mMemory.TimeIndex==-1)
            {
                mMemory.CurrentTime = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
                mMemory.TimeIndex=tid;
            }
            else if (mMemory.TimeIndex != tid)
            {
                AddPatch(mMemory.CurrentTime,mMemory);
                //new memory
                mMemory.Clear();
                mMemory.CurrentTime = new DateTime(time.Year, time.Month, time.Day, ((time.Hour / FileDuration) * FileDuration), 0, 0);
                mMemory.TimeIndex = tid;
            }
            //写入相对时间
            mMemory.Write((int)(time - mMemory.CurrentTime).TotalMilliseconds);
            switch (mMemory.ValueType)
            {
                case (byte)TagType.Bool:
                    mMemory.WriteByte(Convert.ToBoolean(value!=null?value:false)?(byte)1:(byte)0);
                    break;
                case (byte)TagType.Byte:
                    mMemory.WriteByte(Convert.ToByte(value != null ? value : 0));
                    break;
                case (byte)TagType.Short:
                    mMemory.Write(Convert.ToInt16(value != null ? value : 0));
                    break;
                case (byte)TagType.UShort:
                    mMemory.Write(Convert.ToUInt16(value != null ? value : 0));
                    break;
                case (byte)TagType.Int:
                    mMemory.Write(Convert.ToInt32(value != null ? value : 0));
                    break;
                case (byte)TagType.UInt:
                    mMemory.Write(Convert.ToUInt32(value != null ? value : 0));
                    break;
                case (byte)TagType.Long:
                    mMemory.Write(Convert.ToInt64(value != null ? value : 0));
                    break;
                case (byte)TagType.ULong:
                    mMemory.Write(Convert.ToUInt64(value != null ? value : 0));
                    break;
                case (byte)TagType.Double:
                    mMemory.Write(Convert.ToDouble(value != null ? value : 0));
                    break;
                case (byte)TagType.Float:
                    mMemory.Write(Convert.ToSingle(value != null ? value : 0));
                    break;
                case (byte)TagType.String:
                    mMemory.Write(value!=null? Convert.ToString(value):"");
                    break;
                case (byte)TagType.DateTime:
                    mMemory.Write(value!=null? Convert.ToDateTime(value):DateTime.MinValue);
                    break;
                case (byte)TagType.IntPoint:
                    if (value == null) { mMemory.Write(0); mMemory.Write(0); }
                    else
                    {
                        IntPointData data = (IntPointData)value;
                        mMemory.Write(data.X);
                        mMemory.Write(data.Y);
                    }
                    break;
                case (byte)TagType.UIntPoint:
                    if (value == null) { mMemory.Write(0); mMemory.Write(0); }
                    else
                    {
                        UIntPointData udata = (UIntPointData)value;
                        mMemory.Write(udata.X);
                        mMemory.Write(udata.Y);
                    }
                    break;
                case (byte)TagType.IntPoint3:
                    if (value == null) { mMemory.Write(0); mMemory.Write(0); mMemory.Write(0); }
                    else
                    {
                        IntPoint3Data data3 = (IntPoint3Data)value;
                        mMemory.Write(data3.X);
                        mMemory.Write(data3.Y);
                        mMemory.Write(data3.Z);
                    }
                    break;
                case (byte)TagType.UIntPoint3:
                    if (value == null) { mMemory.Write(0); mMemory.Write(0); mMemory.Write(0); }
                    else
                    {
                        UIntPoint3Data udata3 = (UIntPoint3Data)value;
                        mMemory.Write(udata3.X);
                        mMemory.Write(udata3.Y);
                        mMemory.Write(udata3.Z);
                    }
                    break;
                case (byte)TagType.LongPoint:
                    if (value == null) { mMemory.Write(0L); mMemory.Write(0L); }
                    else
                    {
                        LongPointData ldata = (LongPointData)value;
                        mMemory.Write(ldata.X);
                        mMemory.Write(ldata.Y);
                    }
                    break;
                case (byte)TagType.ULongPoint:
                    if (value == null) { mMemory.Write(0L); mMemory.Write(0L); }
                    else
                    {
                        ULongPointData uldata = (ULongPointData)value;
                        mMemory.Write(uldata.X);
                        mMemory.Write(uldata.Y);
                    }
                    break;
                case (byte)TagType.LongPoint3:
                    if (value == null) { mMemory.Write(0L); mMemory.Write(0L); mMemory.Write(0L); }
                    else
                    {
                        LongPoint3Data ldata3 = (LongPoint3Data)value;
                        mMemory.Write(ldata3.X);
                        mMemory.Write(ldata3.Y);
                        mMemory.Write(ldata3.Z);
                    }
                    break;
                case (byte)TagType.ULongPoint3:
                    if (value == null) { mMemory.Write(0L); mMemory.Write(0L); mMemory.Write(0L); }
                    else
                    {
                        ULongPoint3Data uldata3 = (ULongPoint3Data)value;
                        mMemory.Write(uldata3.X);
                        mMemory.Write(uldata3.Y);
                        mMemory.Write(uldata3.Z);
                    }
                    break;
            }
            mMemory.WriteByte(quality);
        }

        /// <summary>
        /// 
        /// </summary>
        public void EndPatch()
        {
            if (mMemory.TimeIndex > -1)
                AddPatch(mMemory.CurrentTime, mMemory);
            mIsBusy = false;
        }

        /// <summary>
        /// 添加绑定
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="timeId"></param>
        /// <param name="hisDataPatch"></param>
        public void AddPatch(DateTime timeId,HisDataPatchMemory hisDataPatch)
        {
            CheckFile(hisDataPatch.Id,timeId);
            FileWriter.GoToEnd();
            AppendData(hisDataPatch);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="hisDataPatch"></param>
        private unsafe void AppendData(HisDataPatchMemory hisDataPatch)
        {
            IntPtr mData = hisDataPatch.GetCompressDatas(out long datacount);
            var userdata = Encoding.UTF8.GetBytes(hisDataPatch.User != null ? hisDataPatch.User : "");
            var infodata = Encoding.UTF8.GetBytes(hisDataPatch.Msg != null ? hisDataPatch.Msg : "");

            long datasize = datacount+5+userdata.Length+infodata.Length+1;
            //写入变量ID
            FileWriter.Write(hisDataPatch.Id, FileWriter.CurrentPostion);
            //写入数据大小
            FileWriter.Write((uint)datasize,FileWriter.CurrentPostion);

            //写入数据
            FileWriter.Write(hisDataPatch.ValueType,FileWriter.CurrentPostion);
            using (System.IO.UnmanagedMemoryStream stream1 = new UnmanagedMemoryStream((byte*)(mData), datacount))
            {
                stream1.CopyTo(FileWriter.GetStream());
            }

            //写入用户信息
            FileWriter.Write((short)userdata.Length,FileWriter.CurrentPostion);
            FileWriter.Write(userdata,FileWriter.CurrentPostion);
            //写入备注信息
            FileWriter.Write((short)infodata.Length, FileWriter.CurrentPostion);
            FileWriter.Write(infodata, FileWriter.CurrentPostion);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class HisDataPatchMemory: MarshalFixedMemoryBlock
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public HisDataPatchMemory(long size):base(size)
        {

        }

        /// <summary>
        /// 值类型
        /// </summary>
        public byte ValueType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int ValueCount { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int ValueAddress { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int QualityAddress { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime CurrentTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int TimeIndex { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string User { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Msg { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IntPtr GetCompressDatas(out long size)
        {
            size = this.Position;
            return this.Buffers;
        }

    }
}
