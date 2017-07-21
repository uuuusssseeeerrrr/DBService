using System;
using System.Data;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;

class DBService
{
    private volatile static DBService _service;
    private MySqlConnection conn = null;

    public static DBService GetInstatce()
    {
        lock (typeof(DBService))
        {
            if (_service == null)
            {
                _service = new DBService();
            }
        }
        return _service;
    }

    /// <summary>
    /// select(파라미터가 없으면 values에 null입력)
    /// </summary>
    public DataTable Select(string sql, object[] values)
    {
        DataTable table = new DataTable();

        if (values == null || arraylength(values) == 0)
        {
            table = SelectAll(sql);
        }
        else
        {
            resizeArray(ref values);
            string[] ParamArray = extractParameters(sql);
            table = SelectWtihParameters(sql, ParamArray, values);
        }
        return table;
    }

    /// <summary>
    /// 파라미터가 존재하는 자료를 SELECT한다
    /// </summary>
    private DataTable SelectWtihParameters(string sql, string[] parameters, object[] values)
    {
        DataTable table = new DataTable();
        DataSet set = new DataSet();
        MySqlDataAdapter adapter = new MySqlDataAdapter();
        try
        {
            using (conn = new MySqlConnection(bleumembership.Properties.Resources.ConnectionString))
            {
                conn.Open();
                MySqlCommand command = new MySqlCommand(sql, conn);
                if (parameters.Length > 0)
                {
                    for (int i = 0; i < parameters.Length; i++)
                    {
                        if (string.IsNullOrEmpty(parameters[i]) == false)
                        {
                            command.Parameters.AddWithValue(parameters[i], values[i].ToString());
                            adapter.SelectCommand = command;
                        }
                    }
                }
                //MySqlDataReader reader = command.ExecuteReader();
                //table.Load(reader);
                adapter.Fill(set);
            }
        }
        catch (Exception ex)
        {
#if DEBUG
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
            Console.WriteLine(ex.InnerException);
#else
            //write yout logs
#endif
        }
        return set.Tables[0];
    }

    /// <summary>
    /// 파라미터가 없는 자료를 SELECT 한다
    /// </summary>
    private DataTable SelectAll(string sql)
    {
        DataSet set = new DataSet();
        try
        {
            using (MySqlDataAdapter adapter = new MySqlDataAdapter(sql, bleumembership.Properties.Resources.ConnectionString))
            {
                adapter.Fill(set);
            }
        }
        catch (Exception ex)
        {
#if DEBUG
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
#else
            //write yout logs
#endif
        }
        return set.Tables[0];
    }


    /// <summary>
    /// 입력한 자료를 저장한다
    /// </summary>
    public string Save(string sql, DataTable table)
    {
        string msg = "";
        try
        {
            using (conn = new MySqlConnection(bleumembership.Properties.Resources.ConnectionString))
            {
                MySqlDataAdapter dataAdapter = new MySqlDataAdapter(sql, conn);
                using (MySqlCommandBuilder builder = new MySqlCommandBuilder(dataAdapter))
                {
                    dataAdapter.Update(table);
                    msg = "저장 되었습니다.";
                }
            }
        }
        catch (Exception ex)
        {
            msg = "저장하지 못했습니다";
#if DEBUG
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
#else
            //write yout logs
#endif
        }
        return msg;
    }

    /// <summary>
    /// sql문을 이용한 삭제
    /// </summary>
    public int Delete(string sql, object[] values)
    {
        int msg = 0;
        string[] parameters = extractParameters(sql);
        try
        {
            using (conn = new MySqlConnection(bleumembership.Properties.Resources.ConnectionString))
            {
                MySqlCommand command = new MySqlCommand(sql, conn);
                conn.Open();
                if (parameters != null && values != null)
                {
                    for (int i = 0; i < parameters.Length; i++)
                    {
                        if (string.IsNullOrEmpty(parameters[i]) == false)
                        {
                            command.Parameters.AddWithValue(parameters[i], values[i]);
                        }
                    }
                }
                msg = command.ExecuteNonQuery();
            }
        }
        catch (Exception ex)
        {
#if DEBUG
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
#else
            //write yout logs
#endif
        }
        return msg;
    }

    /// <summary>
    /// sql문을 이용한 삽입
    /// </summary>
    public int Insert(string sql, object[] values)
    {
        int msg = 0;
        string[] parameters = extractParameters(sql);
        try
        {
            using (conn = new MySqlConnection(bleumembership.Properties.Resources.ConnectionString))
            {
                MySqlCommand command = new MySqlCommand(sql, conn);
                conn.Open();
                if (parameters != null && values != null)
                {
                    for (int i = 0; i < parameters.Length; i++)
                    {
                        if (string.IsNullOrEmpty(parameters[i]) == false)
                        {
                            command.Parameters.AddWithValue(parameters[i], values[i]);
                        }
                    }
                }
                msg = command.ExecuteNonQuery();
            }
        }
        catch (Exception ex)
        {
#if DEBUG
            Console.WriteLine("Insert");
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
            Console.WriteLine(ex.InnerException);
            Console.WriteLine(ex.Source);
#else
            //write yout logs
#endif
        }
        return msg;
    }

    /// <summary>
    /// sql문을 이용한 수정
    /// </summary>
    public int Update(string sql, object[] values)
    {
        int msg = 0;
        string[] parameters = extractParameters(sql);
        try
        {
            using (conn = new MySqlConnection(bleumembership.Properties.Resources.ConnectionString))
            {
                MySqlCommand command = new MySqlCommand(sql, conn);
                conn.Open();
                if (parameters != null && values != null)
                {
                    for (int i = 0; i < parameters.Length; i++)
                    {
                        if (string.IsNullOrEmpty(parameters[i]) == false)
                        {
                            command.Parameters.AddWithValue(parameters[i], values[i]);
                        }
                    }
                }
                msg = command.ExecuteNonQuery();
            }
        }
        catch (Exception ex)
        {
#if DEBUG
            Console.WriteLine("Update");
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
            Console.WriteLine(ex.InnerException);
            Console.WriteLine(ex.Source);
#else
            //write yout logs
#endif
        }
        return msg;
    }

    /// <summary>
    /// 쿼리의 결과를 2차 배열로 리턴
    /// </summary>
    public string[][] GetDataArray(DataTable dt)
    {
        string[][] datas = null;
        try
        {
            int dt_rows_count = dt.Rows.Count;//데이터 테이블의 행 수 얻기
            int dt_cols_count = dt.Columns.Count;//데이터 테이블의 칼럼 수 얻기얻기 

            if (dt_rows_count > 0)//데이터 테이블의 크기가 0보다 크다면
            {
                datas = new string[dt_rows_count][];//행만큼의 2차원 배열 생성
                for (int i = 0; i < dt_rows_count; i++)
                {
                    datas[i] = new string[dt_cols_count];//해당 행만큼 세부 1차원 배열 생성
                }

                for (int i = 0; i < dt_rows_count; i++)
                {
                    for (int j = 0; j < dt_cols_count; j++)
                    {
                        datas[i][j] = Convert.ToString(dt.Rows[i][j]);//데이터를Convert.ToString(dt.Rows[i][j]);//데이터를 생성한 배열에 저장
                    }
                }
            }
            else
            {
                datas = null;//0보다 작다면 null값 설정
            }
        }
        catch (Exception ex)
        {
            //write yout logs
        }
        finally
        {
            //   dt.Dispose();//데이터 테이블 해제
        }
        return datas;//해당 sql문의 쿼리 결과를 2차원 문자열 배열로 전송 
    }

    /// <summary>
    /// sql에 @파라미터가 있을경우 추출(_허용)
    /// </summary>
    private string[] extractParameters(string sql)
    {
        Regex reg = new Regex(@"@[a-zA-Z_]+", RegexOptions.IgnoreCase);
        MatchCollection collection = reg.Matches(sql);
        string[] ParamArray = new string[collection.Count];
        int i = 0;
        foreach (Match m in collection)
        {
            ParamArray.SetValue(m.Value, i);
            i++;
        }
        return ParamArray;
    }
    /// <summary>
    /// 배열에서 빈값이 아닌 개수 리턴
    /// </summary>
    private int arraylength(object[] values)
    {
        int cnt = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] != null)
                cnt++;
        }
        return cnt;
    }

    /// <summary>
    /// 배열 빈칸제거
    /// </summary>
    private void resizeArray(ref object[] values)
    {
        int index = 0;
        object[] backupArray = values;
        Array.Resize<object>(ref values, arraylength(values));
        for(int i=0; i < backupArray.Length; i++)
        {
           if(backupArray[i] != null){ values[index] = backupArray[i]; index++; }
        }
        //return void;
    }
}
