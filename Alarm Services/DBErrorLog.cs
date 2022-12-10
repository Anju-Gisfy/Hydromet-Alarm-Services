using System;
using System.Data;
using System.Configuration;
using System.Linq;
using System.Web;
using System.Xml.Linq;
using Npgsql;
using System.IO;

namespace SHEF_Windows_Service2
{
    public class DBErrorLog
    {
        //public DBErrorLog()
        //{
        //}

        public void AppendToErrorLog(String strErrorMsg, String strFileName, String strFunctionName, String strUserName)
        {
            NpgsqlConnection mNpgsqlConnection = null;
            NpgsqlCommand mNpgsqlCommand = null;

            try
            {

                // DateTime mDateTime = DateTime.Now;

                //mNpgsqlConnection = new NpgsqlConnection(ConfigurationManager.ConnectionStrings["strPostGreSqlConnString"] + ";" + new TestZababIyar().strUser + ";" + new TestZababIyar().strPwd);
                //mNpgsqlCommand = new NpgsqlCommand();
                //mNpgsqlCommand.Connection = mNpgsqlConnection;
                //mNpgsqlConnection.Open();
                //mNpgsqlCommand.CommandType = CommandType.Text;
                //mNpgsqlCommand.CommandText = "INSERT INTO  \"tblErrorLog\" (\"Date\", \"ErrorMsg\", \"FileName\", \"FunctionName\", \"UserName\")  VALUES ('" + mDateTime.Date + "','" + strErrorMsg + "','" + strFileName + "','" + strFunctionName + "','" + strUserName + "')";
                //mNpgsqlCommand.ExecuteNonQuery();

            }
            catch (Exception exc)
            {

            }
            finally
            {
                try
                {
                    mNpgsqlCommand.Dispose();
                    mNpgsqlConnection.Close();
                }
                catch (Exception e)
                { }
            }
        }
        /*Used to excute query which is store in string variable
         * Input Parameters: strQuery-> Query string */
        public void ExecuteQueryHardCoded(String strQuery, String strFileName, String strFunctionName, String strUserName)
        {
            NpgsqlConnection mNpgsqlConnection = null;
            NpgsqlCommand mNpgsqlCommand = null;

            try
            {

                DateTime mDateTime = new DateTime();

                mNpgsqlConnection = new NpgsqlConnection(ConfigurationManager.ConnectionStrings["DBConn"].ConnectionString);
                mNpgsqlCommand = new NpgsqlCommand();
                mNpgsqlCommand.Connection = mNpgsqlConnection;
                mNpgsqlConnection.Open();
                mNpgsqlCommand.CommandType = CommandType.Text;
                mNpgsqlCommand.CommandText = strQuery;
                mNpgsqlCommand.ExecuteNonQuery();

            }
            catch (Exception exc)
            {
                throw exc;

            }
            finally
            {
                try
                {
                    mNpgsqlCommand.Dispose();
                    mNpgsqlConnection.Close();
                }

                catch (Exception e)
                { }
            }
        }
        public void ExecuteQuery(String strQuery, String strFileName, String strFunctionName, String strUserName)
        {
            NpgsqlConnection mNpgsqlConnection = null;
            NpgsqlCommand mNpgsqlCommand = null;

            try
            {

                DateTime mDateTime = new DateTime();
                String conn = ConfigurationManager.ConnectionStrings["DBConn"].ConnectionString;
                mNpgsqlConnection = new NpgsqlConnection(conn);
                mNpgsqlCommand = new NpgsqlCommand();
                mNpgsqlCommand.Connection = mNpgsqlConnection;
                mNpgsqlConnection.Open();
                mNpgsqlCommand.CommandType = CommandType.Text;
                mNpgsqlCommand.CommandText = strQuery;
                mNpgsqlCommand.ExecuteNonQuery();

            }
            catch (Exception exc)
            {
                throw exc;

            }
            finally
            {
                try
                {
                    mNpgsqlCommand.Dispose();
                    mNpgsqlConnection.Close();
                }
                catch (Exception e)
                { }
            }
        }
        /*
        used to retrive station full name
        input parameters : stnname: Station Shef Code
        Return : Station full name 
        Return Datatype : String */
        public String getstationname(String stnname)
        {
            NpgsqlConnection mNpgsqlConnection = null;
            NpgsqlCommand mNpgsqlCommand = null;
            String result = null;
            try
            {
                String conn = ConfigurationManager.ConnectionStrings["DBConn"].ConnectionString;
                mNpgsqlConnection = new NpgsqlConnection(conn);
                mNpgsqlCommand = new NpgsqlCommand();
                mNpgsqlCommand.Connection = mNpgsqlConnection;
                mNpgsqlCommand.CommandTimeout = 0;
                mNpgsqlConnection.Open();
                mNpgsqlCommand.CommandType = CommandType.Text;
                mNpgsqlCommand.CommandText = "select \"Station_Full_Name\" from \"tblAlarm2Sensor\" where \"Station_Shef_Code\"='" + stnname + "'";
                result = mNpgsqlCommand.ExecuteScalar().ToString();
                return result;
            }
            catch (Exception exc)
            {
                //throw exc;
                return exc.Message;
            }
            finally
            {
                try
                {
                    mNpgsqlCommand.Dispose();
                    mNpgsqlConnection.Close();
                }
                catch (Exception e)
                {

                }
            }
        }
        /*
        used to retrive sensor name from sensor shef        
        input parameters : stnname: Station full name,stnshef:  Station Shef Code , senshef: sensor shef code
        Return : Sensor name
        Return Datatype : String */
        public String getsensorname(String stnname, String stnshef, String senshef)
        {
            NpgsqlConnection mNpgsqlConnection = null;
            NpgsqlCommand mNpgsqlCommand = null;
            String result = null;
            try
            {
                String conn = ConfigurationManager.ConnectionStrings["DBConn"].ConnectionString;
                mNpgsqlConnection = new NpgsqlConnection(conn);
                mNpgsqlCommand = new NpgsqlCommand();
                mNpgsqlCommand.Connection = mNpgsqlConnection;
                mNpgsqlCommand.CommandTimeout = 0;
                mNpgsqlConnection.Open();
                mNpgsqlCommand.CommandType = CommandType.Text;
                mNpgsqlCommand.CommandText = "select \"SensorName\" from \"tblAlarm2Sensor\" where \"Station_Shef_Code\"='" + stnshef + "' and \"Station_Full_Name\"='" + stnname + "' and \"Sensor_Shef_Code\"='" + senshef + "'";
                result = mNpgsqlCommand.ExecuteScalar().ToString();
                return result;
            }
            catch (Exception exc)
            {
                //throw exc;
                return exc.Message;
            }
            finally
            {
                try
                {
                    mNpgsqlCommand.Dispose();
                    mNpgsqlConnection.Close();
                }
                catch (Exception e)
                {

                }
            }
        }
        /* Used to excute update query whgich is in string format
         Input parameter: strQuery->query
         returns: No.of rows updated 
         retrun datatype: integer
         */
        public int UpdateQuery(String strQuery, String strFileName, String strFunctionName, String strUserName)
        {
            NpgsqlConnection mNpgsqlConnection = null;
            NpgsqlCommand mNpgsqlCommand = null;

            try
            {

                DateTime mDateTime = new DateTime();

                mNpgsqlConnection = new NpgsqlConnection(ConfigurationManager.ConnectionStrings["DBConn"].ConnectionString);
                mNpgsqlCommand = new NpgsqlCommand();
                mNpgsqlCommand.Connection = mNpgsqlConnection;
                mNpgsqlConnection.Open();
                mNpgsqlCommand.CommandType = CommandType.Text;
                mNpgsqlCommand.CommandText = strQuery;
                return mNpgsqlCommand.ExecuteNonQuery();

            }
            catch (Exception exc)
            {
                //AppendToErrorLog(exc.Message, strFileName, strFunctionName, strUserName);
                return -1;
            }
            finally
            {
                try
                {
                    mNpgsqlCommand.Dispose();
                    mNpgsqlConnection.Close();
                }
                catch (Exception e)
                { }
            }
        }
        /* Used to excute query in StrQuery paramter
        Return type : Dataset */
        public System.Data.DataSet getResultset(String strQuery, String strFileName, String strFunctionName, String strUserName)
        {
            NpgsqlConnection mNpgsqlConnection = null;
            NpgsqlCommand mNpgsqlCommand = null;
            NpgsqlDataAdapter mNpgsqlDataAdapter = null;
            System.Data.DataSet mDataSet = new System.Data.DataSet();


            try
            {

                String conn = ConfigurationManager.ConnectionStrings["DBConn"].ConnectionString;
                mNpgsqlConnection = new NpgsqlConnection(conn);
                mNpgsqlCommand = new NpgsqlCommand();
                mNpgsqlCommand.Connection = mNpgsqlConnection;
                mNpgsqlConnection.Open();
                mNpgsqlCommand.CommandType = CommandType.Text;
                mNpgsqlCommand.CommandText = strQuery;
                mNpgsqlDataAdapter = new NpgsqlDataAdapter(mNpgsqlCommand);
                mNpgsqlDataAdapter.Fill(mDataSet);
                return mDataSet;
            }
            catch (Exception exc)
            {
                //AppendToErrorLog(exc.Message, strFileName, strFunctionName, strUserName);
                return null;
            }
            finally
            {
                try
                {
                    mNpgsqlCommand.Dispose();
                    mNpgsqlConnection.Close();
                }
                catch (Exception e)
                { }
            }

        }
    }
}