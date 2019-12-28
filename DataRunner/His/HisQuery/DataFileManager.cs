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
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class DataFileManager
    {

        #region ... Variables  ...

        private Dictionary<int, YearTimeFile> mTimeFileMaps = new Dictionary<int, YearTimeFile>();

        private string mDatabaseName;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        
        /// <summary>
        /// 
        /// </summary>
        public DataFileManager(string dbname)
        {
            mDatabaseName = dbname;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public async Task Int()
        {
           await Scan(PathHelper.helper.GetDataPath(this.mDatabaseName));
        }

        /// <summary>
        /// 搜索文件
        /// </summary>
        /// <param name="path"></param>
        public async Task Scan(string path)
        {
            System.IO.DirectoryInfo dir = new System.IO.DirectoryInfo(path);
            foreach(var vv in dir.GetFiles())
            {
                if (vv.Extension == SeriseEnginer.DataFileExtends)
                {
                    ParseFileName(vv);
                }
            }
            foreach(var vv in dir.GetDirectories())
            {
                await Scan(vv.FullName);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fileName"></param>
        private void ParseFileName(System.IO.FileInfo file)
        {
            string sname = file.Name.Replace(SeriseEnginer.DataFileExtends,"");
            string stime = sname.Substring(sname.Length - 12, 12);

            int yy = int.Parse(sname.Substring(0, 4));
            int mm = int.Parse(sname.Substring(4, 2));
            int dd = int.Parse(sname.Substring(6, 2));

            int hhspan = int.Parse(sname.Substring(8, 2));
            
            int hhind = int.Parse(sname.Substring(10, 2));

            int hh = 24 / hhspan * hhind;

            YearTimeFile yt = new YearTimeFile() { TimeKey = yy };

            if (mTimeFileMaps.ContainsKey(yy))
            {
                yt = mTimeFileMaps[yy];
            }
            else
            {
                mTimeFileMaps.Add(yy, yt);
            }
            yt.AddMonth(mm).AddDay(dd).AddHour(hh).AddMinutes().ForEach(e=> { e.AddFile(file.FullName); });

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public MinuteTimeFile GetFile(DateTime time)
        {
            if(mTimeFileMaps.ContainsKey(time.Year))
            {
                return mTimeFileMaps[time.Year].GetFile(time);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <returns></returns>
        public List<MinuteTimeFile> GetFiles(DateTime starttime,DateTime endtime)
        {
            List<MinuteTimeFile> re = new List<MinuteTimeFile>();
            DateTime sstart = starttime;
            while (sstart <= endtime)
            {
                re.Add(GetFile(sstart));
                sstart = sstart.AddMinutes(1);
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="times"></param>
        /// <returns></returns>
        public Dictionary<DateTime, MinuteTimeFile> GetFiles(List<DateTime> times)
        {
            Dictionary<DateTime,MinuteTimeFile> re = new Dictionary<DateTime, MinuteTimeFile>();
            foreach(var vv in times)
            {
                re.Add(vv,GetFile(vv));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <returns></returns>
        private DataFileSeriserbase GetFileSerise(string datafile)
        {
            return null;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
