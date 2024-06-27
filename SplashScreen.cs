// SplashScreen.cs
using System;
using System.Threading;
using System.Windows.Forms;

namespace Kpic_DrugStoreSync
{
    public partial class SplashScreen : Form
    {
        delegate void UpdateProgressDelegate(int value);
        delegate void CloseFormDelegate();

        public SplashScreen()
        {
            InitializeComponent();
            progressBar1.Maximum = 100;
            progressBar1.Minimum = 0;
            Thread thread = new Thread(Thread1);
            thread.Start();
        }

        private void UpdateProgress(int value)
        {
            if (InvokeRequired)
            {
                Invoke(new UpdateProgressDelegate(UpdateProgress), value);
            }
            else
            {
                progressBar1.Value = value;
            }
        }

        private void CloseForm()
        {
            if (InvokeRequired)
            {
                Invoke(new CloseFormDelegate(CloseForm));
            }
            else
            {
                this.Close();
            }
        }

        private void Thread1()
        {
            for (int i = 0; i <= 100; i++)
            {
                UpdateProgress(i);
                Thread.Sleep(30);
            }

            CloseForm();
        }

        private void progressBar1_Click(object sender, EventArgs e)
        {
            // 이벤트 처리 코드 (필요 시)
        }
    }
}
