using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace DevelopWebApiDemo
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        DBDevelopClientWebApi.DevelopServiceHelper mHelper;
        private string mCurrentDatabase="";
        private string mTagGroup = "";
        public MainWindow()
        {
            InitializeComponent();
            mHelper = new DBDevelopClientWebApi.DevelopServiceHelper();
        }

        private void loginb_Click(object sender, RoutedEventArgs e)
        {
            mHelper.Server = serverIp.Text;
            mHelper.Login("Admin", "Admin");
        }

        private void getTag_Click(object sender, RoutedEventArgs e)
        {
            int count = 0;
           var tags =  mHelper.GetTagByGroup(mCurrentDatabase, mTagGroup, 0,out count);
            if(tags!=null)
            {
                taglist.ItemsSource = tags.Select(e => e.Item1.Name).ToList();
            }
        }

        private void getTagGroup_Click(object sender, RoutedEventArgs e)
        {
            List<string> ltmp = new List<string>();
            ltmp.Add("");
            var grps = mHelper.GetTagGroup(mCurrentDatabase);
            if(grps!=null)
            {
                foreach(var vv in grps)
                {
                    if(!string.IsNullOrEmpty(vv.Parent))
                    {
                        ltmp.Add(vv.Parent + "." + vv.Name);
                    }
                    else
                    {
                        ltmp.Add(vv.Name);
                    }
                }
            }
            groupList.ItemsSource = ltmp;
            
        }

        private void getDatabase_Click(object sender, RoutedEventArgs e)
        {
            var vdd = mHelper.QueryDatabase();
            if (vdd != null)
            {
                databaseList.ItemsSource = vdd.Select(e => e.Name);
            }
        }

        private void databaseList_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            mCurrentDatabase = databaseList.SelectedItem.ToString();
        }

        private void groupList_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            mTagGroup = groupList.SelectedItem.ToString();
        }

        private void newdtag_Click(object sender, RoutedEventArgs e)
        {
            var vtag = new Cdy.Tag.DoubleTag() { Name = dtagname.Text, Desc = dtagname.Text, LinkAddress = "DirectAccess", ReadWriteType = Cdy.Tag.ReadWriteMode.ReadWrite };
            var htag = new Cdy.Tag.HisTag() { CompressType = 0, Type = Cdy.Tag.RecordType.Timer };
            var vid = mHelper.AddTag(vtag, htag, mCurrentDatabase);
        }

        private void newitag_Click(object sender, RoutedEventArgs e)
        {
            var vtag = new Cdy.Tag.IntTag() { Name = itagname.Text, Desc = dtagname.Text, LinkAddress = "DirectAccess", ReadWriteType = Cdy.Tag.ReadWriteMode.ReadWrite };
            var htag = new Cdy.Tag.HisTag() { CompressType = 0, Type = Cdy.Tag.RecordType.Timer };
            var vid = mHelper.AddTag(vtag, htag, mCurrentDatabase);
        }

        private void saveb_Click(object sender, RoutedEventArgs e)
        {
            mHelper.Save(mCurrentDatabase);
        }
    }
}
