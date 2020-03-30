using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
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

namespace GRPCTests
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            //var client = GetServicClient("127.0.0.1");
            //client.Login(new DBDevelopService.LoginRequest() { UserName = "Admin", Password = "Admin" });
        }


        //private DBDevelopService.DevelopServer.DevelopServerClient GetServicClient(string ip)
        //{
        //    try
        //    {
        //        var httpClientHandler = new HttpClientHandler();
        //        httpClientHandler.ServerCertificateCustomValidationCallback =
        //            HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
        //        var httpClient = new HttpClient(httpClientHandler);

        //        Grpc.Net.Client.GrpcChannel grpcChannel = Grpc.Net.Client.GrpcChannel.ForAddress(@"https://" + ip + ":5001", new GrpcChannelOptions { HttpClient = httpClient });
        //        return new DBDevelopService.DevelopServer.DevelopServerClient(grpcChannel);
        //    }
        //    catch
        //    {
        //        return null;
        //    }
        //}
    }
}
