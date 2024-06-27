// Main.cs
using System;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.Threading;
using System.Windows.Forms;
using System.Data.OracleClient;

namespace Kpic_DrugStoreSync
{
    public partial class Main : Form
    {
        private Thread t1 = null;
        private Thread t2 = null;
        private Thread t3 = null;
        private Boolean isTest = false;

        public Main()
        {
            InitializeComponent();
            toolStripProgressBar1.Maximum = 100;
            toolStripProgressBar1.Minimum = 0;
        }

        private void Main_Load(object sender, EventArgs e)
        {
            toolStripButton3.Enabled = false;
            dateTimePicker1.Value = dateTimePicker1.Value.Date.AddDays(-1);
            dateTimePicker2.Value = dateTimePicker2.Value.Date.AddDays(0);
        }

        private void toolStripButton1_Click(object sender, EventArgs e)
        {
            StartThread(ref t1, LoadData);
        }

        private void toolStripButton2_Click(object sender, EventArgs e)
        {
            StartThread(ref t2, ModifyData);
        }

        private void toolStripButton3_Click(object sender, EventArgs e)
        {
            StartThread(ref t3, InsertData);
        }

        private void StartThread(ref Thread thread, ThreadStart method)
        {
            if (thread != null && thread.IsAlive)
                thread.Abort();

            thread = new Thread(method) { IsBackground = true };
            thread.Start();
        }

        private void LoadData()
        {
            DisableButtons();

            string connectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=117.52.147.9)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=KPAORA)));User Id=kpa;Password=thunder;";
            string queryString = BuildQuery();

            using (var ds = new DataSet())
            {
                ds.Tables.Add("Ori");
                ds.Tables["Ori"].Columns.Add("Oid");
                ds.Tables["Ori"].Columns.Add("지역");
                ds.Tables["Ori"].Columns.Add("약국명");
                ds.Tables["Ori"].Columns.Add("주소1");
                ds.Tables["Ori"].Columns.Add("주소2");
                ds.Tables["Ori"].Columns.Add("이름");
                ds.Tables["Ori"].Columns.Add("주민");
                ds.Tables["Ori"].Columns.Add("전화");
                ds.Tables["Ori"].Columns.Add("면허");
                ds.Tables["Ori"].Columns.Add("우편1");
                ds.Tables["Ori"].Columns.Add("우편2");
                ds.Tables["Ori"].Columns.Add("L");
                ds.Tables["Ori"].Columns.Add("구분");

                string connectionString2 = isTest
                    ? "server=*****;database=db_kpanet-pharm114;uid=*****;pwd=*****"
                    : "server=*****;database=db_kpanet-pharm114;uid=*****;pwd=*****";

                using (var objConn2 = new SqlConnection(connectionString2))
                {
                    try
                    {
                        objConn2.Open();
                        UpdateUI(() => richTextBox1.AppendText($"{DateTime.Now} : 당번약국 DB 접속중입니다...\r\n"));
                        UpdateUI(() => textBox1.Text = "당번약국 DB 접속중...");
                    }
                    catch (Exception ex)
                    {
                        UpdateUI(() => richTextBox1.AppendText($"{DateTime.Now} : {ex.Message}\r\n"));
                        UpdateUI(() => textBox1.Text = "당번약국 DB 접속오류");
                        return;
                    }

                    using (var connection = new OracleConnection(connectionString))
                    {
                        try
                        {
                            connection.Open();
                            var command = new OracleCommand(queryString, connection);
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    string is_fired = GetFireStatus(reader);

                                    string licenseNumber = reader["License_number"].ToString().Trim();
                                    string query = is_fired == "폐업"
                                        ? $"SELECT license FROM drugstore_delete WHERE license = '{licenseNumber}'"
                                        : $"SELECT license FROM drugstore WHERE license = '{licenseNumber}' UNION ALL SELECT license FROM drugstore_delete WHERE license = '{licenseNumber}'";

                                    using (var cmd2 = new SqlCommand(query, objConn2))
                                    {
                                        if (cmd2.ExecuteScalar() == null)
                                        {
                                            AddRowToDataSet(ds.Tables["Ori"], reader, is_fired);
                                        }
                                    }
                                    UpdateProgress(reader);
                                }
                            }
                            UpdateUI(() =>
                            {
                                dataGridView1.DataSource = ds.Tables["Ori"].DefaultView;
                                ConfigureDataGridView(dataGridView1);
                                richTextBox1.AppendText($"{DateTime.Now} : 지정된 날짜 이후의 중복되지 않은 신상 DB 리스트입니다.\r\n");
                                textBox1.Text = "지정된 날짜 이후의 중복되지 않은 신상 DB 리스트입니다.";
                            });
                        }
                        catch (Exception ex)
                        {
                            UpdateUI(() => richTextBox1.AppendText($"{DateTime.Now} : {ex.Message}\r\n"));
                            UpdateUI(() => textBox1.Text = "대한약사회 DB 접속오류");
                        }
                    }
                }
            }

            EnableButtons();
        }

        private void UpdateProgress(OracleDataReader reader)
        {
            UpdateUI(() =>
            {
                toolStripProgressBar1.Maximum = Convert.ToInt32(reader["cnt"]);
                toolStripProgressBar1.Value++;
                textBox1.Text = "대한약사회 DB와 당번약국DB 중복비교중...";
            });
        }

        private void AddRowToDataSet(DataTable table, OracleDataReader reader, string isFired)
        {
            table.Rows.Add(
                reader["OID"],
                reader["REGION_CODE"],
                reader["WORK_PLACE"],
                reader["WORK_ADDR1"],
                reader["WORK_ADDR2"],
                reader["name"],
                reader["jumin1"],
                $"{reader["WORK_TEL1"]}-{reader["WORK_TEL2"]}-{reader["WORK_TEL3"]}",
                reader["License_number"],
                reader["WORK_POST1"],
                reader["WORK_POST2"],
                reader["LAST_DATE"],
                isFired);
        }

        private string GetFireStatus(OracleDataReader reader)
        {
            string isFired = "정상";
            string workCode = reader["work_code"].ToString();
            string postCode = reader["Post_code"].ToString();
            string postMemo = reader["POST_MEMO"].ToString();

            if (workCode != "11" || postCode == "06" || postCode == "07" || postMemo.Contains("폐업") || postMemo.Contains("퇴직"))
            {
                isFired = "폐업";
            }

            return isFired;
        }

        private void ModifyData()
        {
            DisableButtons();
            DataTable modTable = CreateModTable();
            PopulateModTable(modTable);
            UpdateUI(() =>
            {
                dataGridView2.DataSource = modTable.DefaultView;
                ConfigureDataGridView(dataGridView2);
                toolStripProgressBar1.Value = 0;
                richTextBox1.AppendText($"{DateTime.Now} : 데이터 변환이 완료되었습니다. 전송해주세요.\r\n");
                textBox1.Text = "데이터 변환이 완료되었습니다. 전송해주세요.";
            });
            EnableButtons();
        }

        private DataTable CreateModTable()
        {
            var table = new DataTable("Mod");
            table.Columns.Add("Oid");
            table.Columns.Add("지역");
            table.Columns.Add("약국명");
            table.Columns.Add("3");
            table.Columns.Add("시도");
            table.Columns.Add("시군구");
            table.Columns.Add("동면읍");
            table.Columns.Add("나머지주소");
            table.Columns.Add("이름");
            table.Columns.Add("주민");
            table.Columns.Add("전화");
            table.Columns.Add("면허");
            table.Columns.Add("우편1");
            table.Columns.Add("우편2");
            table.Columns.Add("14");
            table.Columns.Add("L");
            table.Columns.Add("구분");
            return table;
        }

        private void PopulateModTable(DataTable modTable)
        {
            UpdateUI(() =>
            {
                toolStripProgressBar1.Minimum = 0;
                toolStripProgressBar1.Maximum = dataGridView1.RowCount;
                toolStripProgressBar1.Value = 0;
                dataGridView1.Enabled = false;
                dataGridView2.Enabled = false;
                richTextBox1.AppendText($"{DateTime.Now} : 데이터 변환중...\r\n");
                textBox1.Text = "데이터 변환중...";
            });

            for (int i = 0; i < dataGridView1.RowCount; i++)
            {
                var row = dataGridView1.Rows[i];
                string newRows = TransformRow(row);
                AddRowToModTable(modTable, newRows, row);
                UpdateUI(() => toolStripProgressBar1.Value++);
            }

            UpdateUI(() =>
            {
                dataGridView1.Enabled = true;
                dataGridView2.Enabled = true;
            });
        }

        private string TransformRow(DataGridViewRow row)
        {
            string oid = row.Cells[0].Value.ToString().Trim();
            string modCol1 = row.Cells[1].Value.ToString().Trim();
            string modCol2 = row.Cells[2].Value.ToString().Trim();
            string address1 = row.Cells[3].Value.ToString().Trim().Replace(" ", "|");
            string address2 = row.Cells[4].Value.ToString().Trim().Replace(" ", "|");
            string newRows = $"{oid}|{modCol1}|{modCol2}|Left|{address1}|{address2}";
            return newRows;
        }

        private void AddRowToModTable(DataTable table, string newRows, DataGridViewRow row)
        {
            string[] columns = newRows.Split('|');
            string isErr = "N";
            if (columns.Length < 16 || string.IsNullOrWhiteSpace(columns[11]))
            {
                isErr = "Y";
            }
            string[] finalRow = new string[table.Columns.Count];
            Array.Copy(columns, finalRow, columns.Length);
            finalRow[14] = isErr;
            finalRow[15] = row.Cells[11].Value.ToString();
            finalRow[16] = row.Cells[12].Value.ToString();
            table.Rows.Add(finalRow);
        }

        private void InsertData()
        {
            DisableButtons();

            using (var objConn = new SqlConnection(GetConnectionString()))
            using (var objConn2 = new SqlConnection(GetConnectionString()))
            {
                objConn.Open();
                objConn2.Open();
                int rowCount = dataGridView2.RowCount;
                UpdateUI(() =>
                {
                    toolStripProgressBar1.Minimum = 0;
                    toolStripProgressBar1.Maximum = rowCount;
                    toolStripProgressBar1.Value = 0;
                });

                for (int i = 0; i < rowCount; i++)
                {
                    var row = dataGridView2.Rows[i];
                    string license = row.Cells[11].Value.ToString().Trim();

                    using (var command = objConn.CreateCommand())
                    {
                        if (row.Cells[16].Value.ToString().Trim() == "폐업")
                        {
                            command.CommandText = BuildDeleteQuery(license);
                            command.ExecuteNonQuery();
                            command.CommandText = BuildInsertQuery(row, "drugstore_delete");
                        }
                        else
                        {
                            command.CommandText = BuildInsertQuery(row, "drugstore");
                        }

                        try
                        {
                            command.ExecuteNonQuery();
                            UpdateUI(() => dataGridView2.Rows.RemoveAt(i));
                        }
                        catch (Exception ex)
                        {
                            UpdateUI(() => richTextBox1.AppendText($"{DateTime.Now} : {ex.Message}\r\n"));
                        }

                        UpdateUI(() => toolStripProgressBar1.Value++);
                    }
                }

                UpdateUI(() =>
                {
                    toolStripProgressBar1.Value = 0;
                    richTextBox1.AppendText($"{DateTime.Now} : 전송이 완료되었습니다.\r\n");
                    textBox1.Text = "전송이 완료되었습니다.";
                });
            }

            EnableButtons();
        }

private string BuildQuery()
{
    var queryBuilder = new StringBuilder();

    queryBuilder.AppendLine("SELECT COUNT (*) OVER () AS cnt,");
    queryBuilder.AppendLine("TO_NUMBER(TO_CHAR (");
    queryBuilder.AppendLine("(CASE");
    queryBuilder.AppendLine("WHEN NVL (U.CRE_DATE, '1-may-1') > NVL (U.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("AND NVL (U.CRE_DATE, '1-may-1') > NVL (P.CRE_DATE, '1-may-1')");
    queryBuilder.AppendLine("AND NVL (U.CRE_DATE, '1-may-1') > NVL (P.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("AND NVL (U.CRE_DATE, '1-may-1') > NVL (W.CRE_DATE, '1-may-1')");
    queryBuilder.AppendLine("AND NVL (U.CRE_DATE, '1-may-1') > NVL (W.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("THEN NVL (U.CRE_DATE, '1-may-1')");
    queryBuilder.AppendLine("WHEN NVL (U.UPD_DATE, '1-may-1') > NVL (P.CRE_DATE, '1-may-1')");
    queryBuilder.AppendLine("AND NVL (U.UPD_DATE, '1-may-1') > NVL (P.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("AND NVL (U.UPD_DATE, '1-may-1') > NVL (W.CRE_DATE, '1-may-1')");
    queryBuilder.AppendLine("AND NVL (U.UPD_DATE, '1-may-1') > NVL (W.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("THEN NVL (U.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("WHEN NVL (P.CRE_DATE, '1-may-1') > NVL (P.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("AND NVL (P.CRE_DATE, '1-may-1') > NVL (W.CRE_DATE, '1-may-1')");
    queryBuilder.AppendLine("AND NVL (P.CRE_DATE, '1-may-1') > NVL (W.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("THEN NVL (P.CRE_DATE, '1-may-1')");
    queryBuilder.AppendLine("WHEN NVL (P.UPD_DATE, '1-may-1') > NVL (W.CRE_DATE, '1-may-1')");
    queryBuilder.AppendLine("AND NVL (P.UPD_DATE, '1-may-1') > NVL (W.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("THEN NVL (P.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("WHEN NVL (W.CRE_DATE, '1-may-1') > NVL (W.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("THEN NVL (W.CRE_DATE, '1-may-1')");
    queryBuilder.AppendLine("ELSE NVL (W.UPD_DATE, '1-may-1')");
    queryBuilder.AppendLine("END), 'YYYYMMDD')) AS LAST_DATE,");
    queryBuilder.AppendLine("U.OID, U.kpa_id, U.License_number, U.license_year, U.REGION_CODE,");
    queryBuilder.AppendLine("TO_CHAR (U.CRE_DATE, 'YYYYMMDD') AS UCRE_DATE,");
    queryBuilder.AppendLine("TO_CHAR (U.UPD_DATE, 'YYYYMMDD') AS UUPD_DATE, P.name, P.jumin1, P.Post_code,");
    queryBuilder.AppendLine("P.Post_memo, P.HP1, P.HP2, P.HP3, TO_CHAR (P.CRE_DATE, 'YYYYMMDD') AS PCRE_DATE,");
    queryBuilder.AppendLine("TO_CHAR (P.UPD_DATE, 'YYYYMMDD') AS PUPD_DATE, W.WORK_PLACE, W.WORK_CODE,");
    queryBuilder.AppendLine("W.WORK_FUNCTION_CODE, W.WORK_POST1, W.WORK_POST2, W.WORK_ADDR1, W.WORK_ADDR2,");
    queryBuilder.AppendLine("W.WORK_TEL1, W.WORK_TEL2, W.WORK_TEL3, W.WORK_FAX1, W.WORK_FAX2, W.WORK_FAX3,");
    queryBuilder.AppendLine("W.SN, TO_CHAR (W.CRE_DATE, 'YYYYMMDD') AS WCRE_DATE,");
    queryBuilder.AppendLine("TO_CHAR (W.UPD_DATE, 'YYYYMMDD') AS WUPD_DATE, D.declaration_date, D.KPA_USER_OID");
    queryBuilder.AppendLine("FROM KPA_USER U");
    queryBuilder.AppendLine("JOIN (SELECT KPA_USER_OID, declaration_date");
    queryBuilder.AppendLine("FROM (SELECT ROW_NUMBER () OVER (PARTITION BY KPA_USER_OID ORDER BY declaration_date DESC) AS cnt,");
    queryBuilder.AppendLine("KPA_USER_OID, declaration_date");
    queryBuilder.AppendLine("FROM KPA_DECLARATION_YEAR");
    queryBuilder.AppendLine("WHERE (declaration_date = '2011' OR declaration_date = '2012')");
    queryBuilder.AppendLine("AND dues_code = '01' AND dbsts = 'A') A WHERE A.cnt = 1) D ON U.OID = D.kpa_user_oid");
    queryBuilder.AppendLine("JOIN KPA_PRIVATE_INFO P ON U.OID = P.OID");
    queryBuilder.AppendLine("JOIN (SELECT * FROM KPA_WORK_INFO WHERE (OID, sn) IN (SELECT OID, MAX (sn) FROM KPA_WORK_INFO GROUP BY OID)) W ON U.OID = W.OID");
    queryBuilder.AppendLine("WHERE (P.POST_CODE NOT IN ('06', '07', '08', '09', '10'))");
    queryBuilder.AppendLine("AND U.dbsts = 'A' AND P.dbsts = 'A' AND W.dbsts = 'A'");
    queryBuilder.AppendLine("AND W.WORK_ADDR1 IS NOT NULL AND W.WORK_ADDR2 IS NOT NULL AND W.WORK_ADDR1 <> '-' AND W.WORK_ADDR2 <> '-' AND W.WORK_ADDR2 <> ' '");
    queryBuilder.AppendFormat("AND TO_NUMBER(TO_CHAR(CASE WHEN NVL(U.CRE_DATE, '1-may-1') > NVL(U.UPD_DATE, '1-may-1') AND NVL(U.CRE_DATE, '1-may-1') > NVL(P.CRE_DATE, '1-may-1') AND NVL(U.CRE_DATE, '1-may-1') > NVL(P.UPD_DATE, '1-may-1') AND NVL(U.CRE_DATE, '1-may-1') > NVL(W.CRE_DATE, '1-may-1') AND NVL(U.CRE_DATE, '1-may-1') > NVL(W.UPD_DATE, '1-may-1') THEN NVL(U.CRE_DATE, '1-may-1') WHEN NVL(U.UPD_DATE, '1-may-1') > NVL(P.CRE_DATE, '1-may-1') AND NVL(U.UPD_DATE, '1-may-1') > NVL(P.UPD_DATE, '1-may-1') AND NVL(U.UPD_DATE, '1-may-1') > NVL(W.CRE_DATE, '1-may-1') AND NVL(U.UPD_DATE, '1-may-1') > NVL(W.UPD_DATE, '1-may-1') THEN NVL(U.UPD_DATE, '1-may-1') WHEN NVL(P.CRE_DATE, '1-may-1') > NVL(P.UPD_DATE, '1-may-1') AND NVL(P.CRE_DATE, '1-may-1') > NVL(W.CRE_DATE, '1-may-1') AND NVL(P.CRE_DATE, '1-may-1') > NVL(W.UPD_DATE, '1-may-1') THEN NVL(P.CRE_DATE, '1-may-1') WHEN NVL(P.UPD_DATE, '1-may-1') > NVL(W.CRE_DATE, '1-may-1') AND NVL(P.UPD_DATE, '1-may-1') > NVL(W.UPD_DATE, '1-may-1') THEN NVL(P.UPD_DATE, '1-may-1') WHEN NVL(W.CRE_DATE, '1-may-1') > NVL(W.UPD_DATE, '1-may-1') THEN NVL(W.CRE_DATE, '1-may-1') ELSE NVL(W.UPD_DATE, '1-may-1') END, 'YYYYMMDD')) >= {0}", dateTimePicker1.Value.Date.ToString("yyyyMMdd"));
    queryBuilder.AppendFormat("AND TO_NUMBER(TO_CHAR(CASE WHEN NVL(U.CRE_DATE, '1-may-1') > NVL(U.UPD_DATE, '1-may-1') AND NVL(U.CRE_DATE, '1-may-1') > NVL(P.CRE_DATE, '1-may-1') AND NVL(U.CRE_DATE, '1-may-1') > NVL(P.UPD_DATE, '1-may-1') AND NVL(U.CRE_DATE, '1-may-1') > NVL(W.CRE_DATE, '1-may-1') AND NVL(U.CRE_DATE, '1-may-1') > NVL(W.UPD_DATE, '1-may-1') THEN NVL(U.CRE_DATE, '1-may-1') WHEN NVL(U.UPD_DATE, '1-may-1') > NVL(P.CRE_DATE, '1-may-1') AND NVL(U.UPD_DATE, '1-may-1') > NVL(P.UPD_DATE, '1-may-1') AND NVL(U.UPD_DATE, '1-may-1') > NVL(W.CRE_DATE, '1-may-1') AND NVL(U.UPD_DATE, '1-may-1') > NVL(W.UPD_DATE, '1-may-1') THEN NVL(U.UPD_DATE, '1-may-1') WHEN NVL(P.CRE_DATE, '1-may-1') > NVL(P.UPD_DATE, '1-may-1') AND NVL(P.CRE_DATE, '1-may-1') > NVL(W.CRE_DATE, '1-may-1') AND NVL(P.CRE_DATE, '1-may-1') > NVL(W.UPD_DATE, '1-may-1') THEN NVL(P.CRE_DATE, '1-may-1') WHEN NVL(P.UPD_DATE, '1-may-1') > NVL(W.CRE_DATE, '1-may-1') AND NVL(P.UPD_DATE, '1-may-1') > NVL(W.UPD_DATE, '1-may-1') THEN NVL(P.UPD_DATE, '1-may-1') WHEN NVL(W.CRE_DATE, '1-may-1') > NVL(W.UPD_DATE, '1-may-1') THEN NVL(W.CRE_DATE, '1-may-1') ELSE NVL(W.UPD_DATE, '1-may-1') END, 'YYYYMMDD')) <= {0}", dateTimePicker2.Value.Date.ToString("yyyyMMdd"));
    queryBuilder.AppendLine("ORDER BY LAST_DATE DESC");

    return queryBuilder.ToString();
}


        private string BuildDeleteQuery(string license)
        {
            return $"DELETE FROM drugstore WHERE license = '{license}'";
        }

        private string BuildInsertQuery(DataGridViewRow row, string tableName)
        {
            return $"INSERT INTO {tableName} ([license], [name], [birthdate], [c_name], [phone], [zip1], [zip2], [address1], [address2], [address3], [address4], [id]) VALUES ('{row.Cells[11].Value}', '{row.Cells[8].Value}', '{row.Cells[9].Value}', '{row.Cells[2].Value}', '{row.Cells[10].Value}', '{row.Cells[12].Value}', '{row.Cells[13].Value}', '{row.Cells[4].Value}', '{row.Cells[5].Value}', '{row.Cells[6].Value}', '{row.Cells[7].Value}', '{row.Cells[1].Value}')";
        }

        private string GetConnectionString()
        {
            return isTest
                ? "server=110.9.251.219;database=db_kpanet-pharm114;uid=mssql;pwd=as8703"
                : "server=117.52.147.26;database=db_kpanet-pharm114;uid=kpanet-pharm114;pwd=kph11419";
        }

        private void DisableButtons()
        {
            UpdateUI(() =>
            {
                toolStripButton1.Enabled = false;
                toolStripButton2.Enabled = false;
                toolStripButton3.Enabled = false;
            });
        }

        private void EnableButtons()
        {
            UpdateUI(() =>
            {
                toolStripButton1.Enabled = true;
                toolStripButton2.Enabled = true;
                toolStripButton3.Enabled = true;
            });
        }

        private void UpdateUI(Action action)
        {
            if (InvokeRequired)
            {
                Invoke(action);
            }
            else
            {
                action();
            }
        }

        private void ConfigureDataGridView(DataGridView dataGridView)
        {
            foreach (DataGridViewColumn column in dataGridView.Columns)
            {
                column.SortMode = DataGridViewColumnSortMode.NotSortable;
            }
        }
    }
}
