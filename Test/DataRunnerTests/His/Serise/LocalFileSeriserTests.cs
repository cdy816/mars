using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cdy.Tag;
//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/1/21 11:55:33.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Tests
{
    [TestClass()]
    public class LocalFileSeriserTests
    {
        [TestMethod()]
        public void AppendTest()
        {
            LocalFileSeriser file = new LocalFileSeriser();
            file.CreatOrOpenFile("test.db");

            List<byte[]> re = new List<byte[]>();
            byte j = 0;
            for(int i=0;i<10;i++)
            {
                byte[] bvals = new byte[10];
                for(int ii=0;ii<bvals.Length;ii++)
                {
                    bvals[ii] = j++;
                }
                re.Add(bvals);
            }

            file.Append(re, 15, 20);
            j = 15;
            int start = 0;
            for (int i = 0; i < 20; i++)
            {
                Assert.IsTrue(j == file.ReadByte(start));
            }
            //var vals = file.ReadByte(0);

            file.Close();


        }
    }
}