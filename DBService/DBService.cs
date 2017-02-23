using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Data;
using MySql.Data.MySqlClient;
using System.Data.SqlClient;

namespace DBService
{
    class DBService
    {
        public const string SQLServer = "SQLServer";
        public const string MySQL = "MySQL";
        private static string connectionType = "";
        private static string connectionString = "";
        string ConnectionString { get { return connectionString; } set { connectionString = value; } }
        private volatile static DBService _service;
        private MySqlConnection mysqlConn = null;
        private SqlConnection sqlConn = null;

        /// <summary>
        /// DBService init(custom Concection)
        /// </summary>
        /// <param name="TypeStr">DBMS(SQLServer, MySQL)</param>
        /// <param name="ConnStr">Connection String</param>
        public static void init(string TypeStr, string ConnStr)
        {
            if (string.IsNullOrWhiteSpace(TypeStr) == false && string.IsNullOrWhiteSpace(ConnStr) == false)
            {
                connectionType = TypeStr;
                _service.ConnectionString = ConnStr;
            }
            else
            {
                throw new Exception("TypeStr & ConnStr Is Null");
            }
        }

        /// <summary>
        /// DBService init(default Concection)
        /// </summary>
        /// <param name="TypeStr">DBMS(SQLServer, MySQL)</param>
        /// <param name="hostIP">Connection IP</param>
        /// <param name="port">Connection Port(Nullable)</param>
        /// <param name="id">Connection ID</param>
        /// <param name="password">Connection Password</param>
        public static void init(string TypeStr, string hostIP, string port, string id, string password, string DBName)
        {
            if (string.IsNullOrWhiteSpace(TypeStr) == false
                && string.IsNullOrWhiteSpace(hostIP) == false
                && string.IsNullOrWhiteSpace(id) == false
                && string.IsNullOrWhiteSpace(password) == false
                && string.IsNullOrWhiteSpace(DBName) == false)
            {
                connectionType = TypeStr;
                switch (TypeStr)
                {
                    case DBService.SQLServer:
                        if (string.IsNullOrWhiteSpace(port) == false)
                            _service.ConnectionString = string.Format("Data Source={0},{1};Initial Catalog={4};Persist Security Info=True;User ID={2};Password={3}", hostIP, port, id, password, DBName);
                        else
                            _service.ConnectionString = string.Format("Data Source={0};Initial Catalog={3};Persist Security Info=True;User ID={1};Password={2}", hostIP, id, password, DBName);
                        break;
                    case DBService.MySQL:
                        if (string.IsNullOrWhiteSpace(port) == false)
                        {
                            _service.ConnectionString = string.Format("Server={0};Port={1};Database={2};Uid={3};Pwd={4}", hostIP, port, DBName, id, password);
                        }
                        else
                        {
                            _service.ConnectionString = string.Format("Server={0};Database={1};Uid={2};Pwd={3}", hostIP, DBName, id, password);
                        }
                        break;
                }
            }
            else
            {
                throw new Exception("Parameter Is Null");
            }
        }

        public static void GetInstance()
        {
            try 
            { 
                 lock (typeof(DBService))
                {
                    if (_service == null)
                    {
                        _service = new DBService();
                    }
                }
            } catch(Exception){
                throw new Exception("init Plz");
            }
               
        }

        /// <summary>
        /// 조건이 없는 select문
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <returns>검색결과</returns>
        public DataTable SelectAll(string sql)
        {
            DataTable table = new DataTable();

            switch (connectionType)
            {
                case DBService.MySQL:
                    try
                    {
                        using (mysqlConn = new MySqlConnection(this.ConnectionString))
                        {
                            mysqlConn.Open();
                            using (MySqlCommand command = mysqlConn.CreateCommand())
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                command.CommandText = sql;
                                table.Load(reader);
                            }
                        }
                    }
                    catch (MySqlException ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.InnerException);
                        Console.WriteLine(ex.StackTrace);
                    }
                    break;
                case DBService.SQLServer:
                    try
                    {
                        using (sqlConn = new SqlConnection(this.ConnectionString))
                        {
                            sqlConn.Open();
                            using (SqlCommand command = sqlConn.CreateCommand())
                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                command.CommandText = sql;
                                table.Load(reader);
                            }
                        }
                    }
                    catch (SqlException ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.InnerException);
                        Console.WriteLine(ex.StackTrace);
                    }
                    break;
            }
            return table;
        }

        /// <summary>
        /// 조건이 존재하는 select문
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <param name="parameters">parameter(string array)</param>
        /// <param name="values">values(string array)</param>
        /// <returns>검색결과</returns>
        public DataTable Select(string sql, string[] parameters, object[] values)
        {
            DataTable table = new DataTable();
            switch (connectionType)
            {
                case DBService.MySQL:
                    try
                    {
                        using (mysqlConn = new MySqlConnection(this.ConnectionString))
                        {
                            mysqlConn.Open();
                            using (MySqlCommand command = mysqlConn.CreateCommand())
                            {
                                command.CommandText = sql;
                                for (int i = 0; i < parameters.Length; i++)
                                {
                                    if (string.IsNullOrEmpty(parameters[i]) == false)
                                    {
                                        command.Parameters.AddWithValue("@" + parameters[i], values[i]);
                                    }
                                }
                                using (MySqlDataReader reader = command.ExecuteReader())
                                {
                                    table.Load(reader);
                                }
                            }
                        }
                    }
                    catch (MySqlException ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.InnerException);
                        Console.WriteLine(ex.StackTrace);
                    }
                    break;
                case DBService.SQLServer:
                    try
                    {
                        using (sqlConn = new SqlConnection(this.ConnectionString))
                        {
                            sqlConn.Open();
                            using (SqlCommand command = sqlConn.CreateCommand())
                            {
                                command.CommandText = sql;
                                for (int i = 0; i < parameters.Length; i++)
                                {
                                    if (string.IsNullOrEmpty(parameters[i]) == false)
                                    {
                                        command.Parameters.AddWithValue("@" + parameters[i], values[i]);
                                    }
                                }
                                using (SqlDataReader reader = command.ExecuteReader())
                                {
                                    table.Load(reader);
                                }
                            }
                        }
                    }
                    catch (SqlException ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.InnerException);
                        Console.WriteLine(ex.StackTrace);
                    }
                    break;
            }
            return table;
        }

        /// <summary>
        /// 출력행이 하나일때 배열로 받음
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <param name="parameters">parameter(string array)</param>
        /// <param name="values">values(string array)</param>
        /// <returns>검색결과(string array)</returns>
        public string[] SelectOne(string sql, string[] parameters, object[] values)
        {
            DataTable table = new DataTable();
            string[] resArray;
            switch (connectionType)
            {
                case DBService.MySQL:
                    try
                    {
                        using (mysqlConn = new MySqlConnection(this.ConnectionString))
                        {
                            mysqlConn.Open();
                            using (MySqlCommand command = mysqlConn.CreateCommand())
                            {

                                for (int i = 0; i < parameters.Length; i++)
                                {
                                    if (string.IsNullOrEmpty(parameters[i]) == false)
                                    {
                                        command.Parameters.AddWithValue("@" + parameters[i], values[i]);
                                    }
                                }
                                using (MySqlDataReader reader = command.ExecuteReader())
                                {
                                    table.Load(reader);
                                }
                            }
                        }
                    }
                    catch (MySqlException ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.InnerException);
                        Console.WriteLine(ex.StackTrace);
                    }
                    break;
                case DBService.SQLServer:
                    try
                    {
                        using (sqlConn = new SqlConnection(this.ConnectionString))
                        {
                            sqlConn.Open();
                            using (SqlCommand command = new SqlCommand(sql, sqlConn))
                            {
                                for (int i = 0; i < parameters.Length; i++)
                                {
                                    if (string.IsNullOrEmpty(parameters[i]) == false)
                                    {
                                        command.Parameters.AddWithValue("@" + parameters[i], values[i]);
                                    }
                                }
                                using (SqlDataReader reader = command.ExecuteReader())
                                {
                                    table.Load(reader);
                                }
                            }
                        }
                    }
                    catch (SqlException ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.InnerException);
                        Console.WriteLine(ex.StackTrace);
                    }
                    break;
            }
            resArray = new string[table.Rows.Count - 1];

            for (int i = 0; i < table.Rows.Count; i++)
            {
                resArray[i] = table.Rows[i][0].ToString();
            }

            return resArray;
        }

        /// <summary>
        /// Insert
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <param name="parameters">parameter(string array)</param>
        /// <param name="values">values(string array)</param>
        /// <returns>검색결과(string array)</returns>
        public int Insert(string sql, string[] parameters, object[] values)
        {
            return IUD(sql, parameters, values);
        }

        /// <summary>
        /// Update
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <param name="parameters">parameter(string array)</param>
        /// <param name="values">values(string array)</param>
        /// <returns>검색결과(string array)</returns>
        public int Update(string sql, string[] parameters, object[] values)
        {
            return IUD(sql, parameters, values);
        }

        /// <summary>
        /// Delete
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <param name="parameters">parameter(string array)</param>
        /// <param name="values">values(string array)</param>
        /// <returns>검색결과(string array)</returns>
        public int Delete(string sql, string[] parameters, object[] values)
        {
            return IUD(sql, parameters, values);
        }

        /// <summary>
        /// Insert, Update, Delete (Inner Method)
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <param name="parameters">parameter(string array)</param>
        /// <param name="values">values(string array)</param>
        /// <returns>검색결과(string array)</returns>
        private int IUD(string sql, string[] parameters, object[] values)
        {
            int msg = 0;
            switch (connectionType)
            {
                case DBService.MySQL:
                    try
                    {
                        using (mysqlConn = new MySqlConnection(this.ConnectionString))
                        {
                            mysqlConn.Open();
                            using (MySqlCommand command = new MySqlCommand(sql, mysqlConn))
                            {
                                if (parameters != null && values != null)
                                {
                                    for (int i = 0; i < parameters.Length; i++)
                                    {
                                        if (string.IsNullOrEmpty(parameters[i]) == false)
                                        {
                                            command.Parameters.AddWithValue("@" + parameters[i], values[i]);
                                        }
                                    }
                                }
                                msg = command.ExecuteNonQuery();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.StackTrace);
                    }
                    break;
                case DBService.SQLServer:
                    try
                    {
                        using (sqlConn = new SqlConnection(this.ConnectionString))
                        {
                            sqlConn.Open();
                            using (SqlCommand command = new SqlCommand(sql, sqlConn))
                            {
                                if (parameters != null && values != null)
                                {
                                    for (int i = 0; i < parameters.Length; i++)
                                    {
                                        if (string.IsNullOrEmpty(parameters[i]) == false)
                                        {
                                            command.Parameters.AddWithValue("@" + parameters[i], values[i]);
                                        }
                                    }
                                }
                                msg = command.ExecuteNonQuery();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.StackTrace);
                    }
                    break;
            }
            return msg;
        }
    }
}
