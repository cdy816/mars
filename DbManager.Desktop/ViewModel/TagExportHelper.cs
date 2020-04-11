//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/7 11:28:35.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using Cdy.Tag;

namespace DBInStudio.Desktop.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public static class TagExportHelper
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="file"></param>
        public static void ExportToCSV(this List<TagViewModel> tags,string file)
        {
            System.IO.StreamWriter writer = new System.IO.StreamWriter(System.IO.File.Open(file, System.IO.FileMode.CreateNew,System.IO.FileAccess.ReadWrite,System.IO.FileShare.ReadWrite),Encoding.Unicode);

            foreach(var vv in tags)
            {
                writer.WriteLine(vv.SaveToCSV());
            }
            writer.Flush();
            writer.Close();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public static string SaveToCSV(this TagViewModel tag)
        {
            string re = string.Empty;
            re = tag.RealTagMode.Id + "," + tag.RealTagMode.Name + "," + tag.RealTagMode.Desc + "," + tag.RealTagMode.Type.ToString() + "," + tag.RealTagMode.LinkAddress + "," + tag.RealTagMode.Group;
            if (tag.HisTagMode != null)
            {
                re += ";" + tag.HisTagMode.Type + "," + tag.HisTagMode.Circle + "," + tag.HisTagMode.CompressType;
                if (tag.HisTagMode.Parameters != null && tag.HisTagMode.Parameters.Count > 0)
                {
                    foreach (var vv in tag.HisTagMode.Parameters)
                    {
                        re += "," + vv.Key + "," + vv.Value;
                    }
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> LoadFromCSV(string value)
        {
            Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> re;
            Cdy.Tag.Tagbase realtag = null;
            Cdy.Tag.HisTag histag = null;
            
            var strs = value.Split(new char[] { ';' });
            string[] sval = strs[0].Split(new char[] { ',' });
            realtag = Cdy.Tag.TagTypeExtends.GetTag((Cdy.Tag.TagType)Enum.Parse(typeof(Cdy.Tag.TagType), sval[3]));
            realtag.Id = int.Parse(sval[0]);
            realtag.Name = sval[1];
            re = new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(realtag, histag);
            return re;
        }




    }
}
