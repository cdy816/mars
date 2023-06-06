using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Linq;

namespace Cdy.Tag
{
    public static class FileHelper
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="doc"></param>
        /// <param name="path"></param>
        /// <param name="callName"></param>
        /// <returns></returns>
        public static bool SaveXMLToFile(this XElement doc, string path, string callName="")
        {
            string ofile = "";

            try
            {
                string sd = System.IO.Path.GetDirectoryName(path);
                if (!System.IO.Directory.Exists(sd))
                {
                    System.IO.Directory.CreateDirectory(sd);
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro(callName, $"{ex.Message} {ex.StackTrace}");
                return false;
            }

            try
            {
                
                if (System.IO.File.Exists(path))
                {
                    ofile = path + "_b";
                    if (System.IO.File.Exists(ofile))
                    {
                        System.IO.File.Delete(ofile);
                    }

                    System.IO.File.Move(path, ofile);
                }
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro(callName, $"{ex.Message} {ex.StackTrace}");
                return false;
            }

            try
            {
                using (var spath = System.IO.File.Open(path, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.ReadWrite, System.IO.FileShare.ReadWrite))
                {
                    doc.Save(spath);
                }

            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro(callName, $"{ex.Message}  {ex.StackTrace}");
                if (!string.IsNullOrEmpty(ofile) && System.IO.File.Exists(ofile))
                {
                    try
                    {
                        if (System.IO.File.Exists(path))
                        {
                            System.IO.File.Delete(path);
                        }
                        System.IO.File.Move(ofile, path);
                    }
                    catch (Exception eex)
                    {
                        LoggerService.Service.Erro(callName, $"{eex.Message}  {eex.StackTrace}");
                    }
                }

                return false;
            }

            try
            {
                if (!string.IsNullOrEmpty(ofile) && System.IO.File.Exists(ofile))
                {
                    System.IO.File.Delete(ofile);
                }
            }
            catch
            {

            }
            return true;
        }
    }
}
