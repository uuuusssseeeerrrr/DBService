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
        private readonly string ConnectionType = "";
        private string connectionString = "";
        string ConnectionString { get { return connectionString; } set { connectionString = value; } }
        private volatile static DBService _service;
        private MySqlConnection mysqlConn = null;
        private SqlConnection sqlConn = null;

        private DBService(string TypeStr)
        {
            switch (TypeStr)
            {
                case DBService.SQLServer:
                    ConnectionType = DBService.SQLServer;
                    break;
                case DBService.MySQL:
                    ConnectionType = DBService.MySQL;
                    break;
                default:
                    throw new Exception("I don't know DBType");
            }
        }

        /// <summary>
        /// DBService 초기설정
        /// </summary>
        /// <param name="TypeStr">DBMS 종류(SQLServer, MySQL)</param>
        /// <param name="ConnStr">DB연결 문자열</param>
        public static DBService GetInstatce(string TypeStr, string ConnStr)
        {
            if (string.IsNullOrWhiteSpace(TypeStr) == false && string.IsNullOrWhiteSpace(ConnStr) == false)
            {
                lock (typeof(DBService))
                {
                    if (_service == null)
                    {
                        _service = new DBService(TypeStr);
                    }
                }
                _service.ConnectionString = ConnStr;
                return _service;
            }
            else
            {
                throw new Exception("TypeStr & ConnStr Is Null");
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

            switch (ConnectionType)
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
            switch (ConnectionType)
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
            switch (ConnectionType)
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
            resArray = new string[table.Rows.Count-1];

            for (int i=0;i<table.Rows.Count;i++)
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
        /// Insert, Update, Delete
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <param name="parameters">parameter(string array)</param>
        /// <param name="values">values(string array)</param>
        /// <returns>검색결과(string array)</returns>
        private int IUD(string sql, string[] parameters, object[] values)
        {
            int msg = 0;
            switch (ConnectionType)
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
