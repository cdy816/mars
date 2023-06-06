//==============================================================
//  Copyright (C) 2020 Chongdaoyang Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/6/19 21:27:03 .
//  Version 1.0
//  CDYWORK
//==============================================================

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Cdy.Tag.Common.Common
{
    public class Md5Helper
    {

        public static string Encode(string data)
        {
            return Encode(data, "mrdbcdy0", "20200619");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="Key_64"></param>
        /// <param name="Iv_64"></param>
        /// <returns></returns>
        public static string Encode(string data, string Key_64, string Iv_64)
        {
            string KEY_64 = Key_64;// "VavicApp";
            string IV_64 = Iv_64;// "VavicApp";

            try
            {
                byte[] byKey = System.Text.ASCIIEncoding.ASCII.GetBytes(KEY_64);
                byte[] byIV = System.Text.ASCIIEncoding.ASCII.GetBytes(IV_64);
                DESCryptoServiceProvider cryptoProvider = new DESCryptoServiceProvider();
                int i = cryptoProvider.KeySize;
                MemoryStream ms = new MemoryStream();
                CryptoStream cst = new CryptoStream(ms, cryptoProvider.CreateEncryptor(byKey, byIV), CryptoStreamMode.Write);
                StreamWriter sw = new StreamWriter(cst);
                sw.Write(data);
                sw.Flush();
                cst.FlushFinalBlock();
                sw.Flush();
                return Convert.ToBase64String(ms.GetBuffer(), 0, (int)ms.Length);
            }
            catch (Exception x)
            {
                return x.Message;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static string Decode(string data)
        {
            return Decode(data, "mrdbcdy0", "20200619");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="Key_64"></param>
        /// <param name="Iv_64"></param>
        /// <returns></returns>
        public static string Decode(string data, string Key_64, string Iv_64)
        {
            string KEY_64 = Key_64;// "VavicApp";密钥
            string IV_64 = Iv_64;// "VavicApp"; 向量
            try
            {
                byte[] byKey = System.Text.ASCIIEncoding.ASCII.GetBytes(KEY_64);
                byte[] byIV = System.Text.ASCIIEncoding.ASCII.GetBytes(IV_64);
                byte[] byEnc;
                byEnc = Convert.FromBase64String(data); //把需要解密的字符串转为8位无符号数组
                DESCryptoServiceProvider cryptoProvider = new DESCryptoServiceProvider();
                MemoryStream ms = new MemoryStream(byEnc);
                CryptoStream cst = new CryptoStream(ms, cryptoProvider.CreateDecryptor(byKey, byIV), CryptoStreamMode.Read);
                StreamReader sr = new StreamReader(cst);
                return sr.ReadToEnd();
            }
            catch (Exception x)
            {
                return x.Message;
            }
        }

        /// <summary>
        /// 计算Sha1的值
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string CalSha1(byte[] value)
        {
            var sha1 = System.Security.Cryptography.SHA1.Create();
            var data = sha1.ComputeHash(value);
            StringBuilder sha1Num = new StringBuilder();
            foreach (var t in data)
            {
                sha1Num.Append(t.ToString("X2"));
            }
            return sha1Num.ToString();
        }

        /// <summary>
        /// 计算入口程序Sha1值
        /// </summary>
        /// <returns></returns>
        public static string CalSha1()
        {
            string sile = Process.GetCurrentProcess().MainModule.FileName;
           
            var sf = sile.Replace(".exe", ".dll");
            if (System.IO.File.Exists(sf))
            {
                sile = sf;
            }
            return CalSha1(System.IO.File.ReadAllBytes(sile));
        }
    }
}
