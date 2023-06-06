using Cdy.Tag;
using System;
using System.Diagnostics;
using System.Linq;
using System.Xml.Linq;

namespace DBHighApi
{
    class Program
    {
        private static bool mIsClosed = false;

        static void Main(string[] args)
        {

            Console.Title = "FileCodeGenerator";
           
            while (!mIsClosed)
            {
                Console.Write("直接输入要计算的文件的路径:");
                string smd = Console.ReadLine();
                if (mIsClosed)
                {
                    break;
                }
                switch (smd)
                {
                    case "exit":
                        mIsClosed = true;
                        break;
                    default:
                        if (System.IO.File.Exists(smd))
                        {
                            string scode = Cdy.Tag.Common.Common.Md5Helper.CalSha1(System.IO.File.ReadAllBytes(smd));
                            Console.WriteLine("ApplicationCode: " + scode);
                        }
                        else
                        {
                            Console.WriteLine("文件不存在!");
                        }
                        break;
                }
            }
        }

        
    }
}
