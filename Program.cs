using System;
using System.Collections.Generic;
using System.Windows.Forms;

namespace Kpic_DrugStoreSync
{
    static class Program
    {
        /// <summary>
        /// 해당 응용 프로그램의 주 진입점입니다.
        /// </summary>
        [STAThread]
        static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            SplashScreen splashScreen = new SplashScreen();
            Application.Run(splashScreen);
            Application.Run(new Main());

        }
    }
}
