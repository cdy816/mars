﻿//------------------------------------------------------------------------------
// <auto-generated>
//     此代码由工具生成。
//     运行时版本:4.0.30319.42000
//
//     对此文件的更改可能会导致不正确的行为，并且如果
//     重新生成代码，这些更改将会丢失。
// </auto-generated>
//------------------------------------------------------------------------------

namespace DBHisDataServer.Properties {
    using System;
    
    
    /// <summary>
    ///   一个强类型的资源类，用于查找本地化的字符串等。
    /// </summary>
    // 此类是由 StronglyTypedResourceBuilder
    // 类通过类似于 ResGen 或 Visual Studio 的工具自动生成的。
    // 若要添加或移除成员，请编辑 .ResX 文件，然后重新运行 ResGen
    // (以 /str 作为命令选项)，或重新生成 VS 项目。
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "17.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    internal class Resources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal Resources() {
        }
        
        /// <summary>
        ///   返回此类使用的缓存的 ResourceManager 实例。
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("DBHisDataServer.Properties.Resources", typeof(Resources).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   重写当前线程的 CurrentUICulture 属性，对
        ///   使用此强类型资源类的所有资源查找执行重写。
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   查找类似 press any key to exit. 的本地化字符串。
        /// </summary>
        internal static string AnyKeyToExit {
            get {
                return ResourceManager.GetString("AnyKeyToExit", resourceCulture);
            }
        }
        
        /// <summary>
        ///   查找类似 Enter h for command help information 的本地化字符串。
        /// </summary>
        internal static string HelpMsg {
            get {
                return ResourceManager.GetString("HelpMsg", resourceCulture);
            }
        }
        
        /// <summary>
        ///   查找类似 display command list 的本地化字符串。
        /// </summary>
        internal static string HMsg {
            get {
                return ResourceManager.GetString("HMsg", resourceCulture);
            }
        }
        
        /// <summary>
        ///   查找类似 start to run a databse,ignor database name to run default &apos;local&apos; database 的本地化字符串。
        /// </summary>
        internal static string StartMsg {
            get {
                return ResourceManager.GetString("StartMsg", resourceCulture);
            }
        }
        
        /// <summary>
        ///   查找类似 stop databse 的本地化字符串。
        /// </summary>
        internal static string StopMsg {
            get {
                return ResourceManager.GetString("StopMsg", resourceCulture);
            }
        }
        
        /// <summary>
        ///   查找类似 ***************Welcome to Mars  database his data server*************** 的本地化字符串。
        /// </summary>
        internal static string WelcomeMsg {
            get {
                return ResourceManager.GetString("WelcomeMsg", resourceCulture);
            }
        }
    }
}
