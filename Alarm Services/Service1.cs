using SHEF_Windows_Service2;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Net;
using System.Net.Mail;
using System.ServiceProcess;
using System.Threading;


namespace Alarm_Services
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            //constructor used to call the below method while creating object of the class  
            InitializeComponent();
        }
        ~Service1()
        {

        }
        StreamWriter logfile; // used to store errors and important info 
        bool bRunThread = false;

        string subPath = "logs";
        public void onDebug()
        {
            OnStart(null);
        }
        static Hashtable condition = new Hashtable();//store the pervious time when the condition is occured 
        static Hashtable DeadbandMet = new Hashtable();//store the pervious time when the condition is occured 

        DBErrorLog db = new DBErrorLog();//creating a object of DBErrorLog to excute the database operations which or in DBErrorLog.cs file
        bool bFirstTime = true;
        // bool serviceStarted;
        Thread faxWorkerThread; //Thread declaration
        protected override void OnStart(string[] args)
        {
            //on start of the service OnStart method excutes we are overriding his function with our custom code
            try
            {
                subPath = System.Reflection.Assembly.GetEntryAssembly().Location;//creating subpath in debug--AlarmTest.exe.logs
                subPath = subPath.Replace("Alarm Services.exe", "");//Replaced subpath with Alarm Services.exe
                subPath = @"" + subPath + "logs";
                bool exists = Directory.Exists(subPath);// check the path is in the directory or not
                bRunThread = true;
                if (!exists)// if not exist create subpath in the directory, here bool=false
                    Directory.CreateDirectory(subPath);//creates the file and floders in subpath 
                //creating a log file to store the process and errors occured while excuting 
                logfile = new StreamWriter(subPath + "\\logfile " + DateTime.Now.Year + "" + DateTime.Now.Month + "" + DateTime.Now.Day + ".txt");


                if (bFirstTime == true)
                {
                    nullify();
                    bFirstTime = false;
                }
                ThreadStart start = new ThreadStart(startAlarm); // FaxWorker is where the work gets done
                faxWorkerThread = new Thread(start); // creating object of thread with threadstart object

                // set flag to indicate worker thread is active
                //  serviceStarted = true;

                // start threads
                faxWorkerThread.Start(); // starts the thread by calling Start() function 
            }
            catch (Exception ex)
            {

            }
        }

        public void nullify()
        {
            try
            {

                subPath = System.Reflection.Assembly.GetEntryAssembly().Location; //creating subpath in debug--AlarmTest.exe.logs
                subPath = subPath.Replace("Alarm Services.exe", "");//Replaced subpath with Alarm Services.exe
                subPath = @"" + subPath + "logs";
                bool exists = Directory.Exists(subPath); // check the path is in the directory or not

                if (!exists) // if not exist create subpath in the directory, here bool=false
                    Directory.CreateDirectory(subPath);

                DataSet ds = db.getResultset("select * from \"tblAlarm2Sensor\" where \"Validity\" = 1", "", "", "");// created dataset ds
                if (ds != null)//if datatable is not null
                {

                    for (int i = 0; i < ds.Tables[0].Rows.Count; i++)//loop datatable
                    {
                        String station_shef_code = ds.Tables[0].Rows[i]["Station_Shef_Code"].ToString().Trim();

                        DataSet sen_id_set = db.getResultset("select \"HydroMetParamsTypeId\" from \"tblHydroMetParamsType\" where \"HydroMetShefCode\"='" + ds.Tables[0].Rows[i]["Sensor_Shef_Code"].ToString().Trim() + "'", "", "", "");
                        //call setAlarmFlag211()
                        setAlarmFlag211(ds.Tables[0].Rows[i]["Station_Shef_Code"].ToString().Trim(), sen_id_set.Tables[0].Rows[0]["HydroMetParamsTypeId"].ToString().Trim());

                    }
                }
            }
            catch (Exception exc)
            {
                WriteToFile("Exception in nullify");
                WriteToFile(exc.Message);
                //logfile.Flush();
                WriteToFile(exc.StackTrace);
                //logfile.Flush();
            }


        }
        //alarm function starts while creating of thread 
        public void startAlarm()
        {
            while (bRunThread)
            {
                try
                {
                    DataSet ds = db.getResultset("select * from \"tblAlarm2Sensor\" where \"Validity\" = 1 and \"EmailSent\"='false' order by \"ID\"", "", "", "");

                    DataSet dn = db.getResultset("select * from \"tblAlarm2Sensor\" where \"Validity\" = 1 and \"AlarmType\"='multiple' and \"EmailSent\"='false' order by \"ID\"", "", "", "");
                    for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                    {
                        DataSet sen_id_set = db.getResultset("select \"HydroMetParamsTypeId\" from \"tblHydroMetParamsType\" where \"HydroMetShefCode\"='" + ds.Tables[0].Rows[i]["Sensor_Shef_Code"].ToString().Trim() + "'", "", "", "");

                        String sen_id = sen_id_set.Tables[0].Rows[0][0].ToString().Trim();
                        condition[sen_id + "" + ds.Tables[0].Rows[i]["Station_Shef_Code"].ToString().Trim()] = true;
                    }
                    if (dn.Tables[0].Rows.Count > 0) 
                    {
                        if (dn.Tables[0].Rows[0]["AlarmType"].ToString() == "multiple")
                        {
                            String qryOldFlag1 = "UPDATE public.\"tblAlarm2Sensor\" SET  \"EmailSent\"='true' WHERE \"Validity\"= 1 and \"AlarmType\"='multiple'";

                            try
                            {
                                db.ExecuteQuery(qryOldFlag1, "", "", "");
                            }
                            catch (Exception e) { }

                            DataTable dt = new DataTable();
                            dt = dn.Tables[0];
                            multialarm(dt);
                        }
                    }
                    
                    for (int i = 0; i < ds.Tables[0].Rows.Count; i++)//loop in datatable 
                    {
                        
                        if (ds.Tables[0].Rows[i]["AlarmType"].ToString() == "single")
                            {
                            String dateto = ds.Tables[0].Rows[i]["RangeTo"].ToString().Trim();
                            String datefrom = ds.Tables[0].Rows[i]["range"].ToString().Trim();
                            String shefcode = ds.Tables[0].Rows[i]["Station_Shef_Code"].ToString().Trim();

                            if (datefrom != "" && dateto != "")// if date is not null
                            {
                                DateTime to = Convert.ToDateTime(dateto); // convert dateto string to datetime variable 
                                DateTime from = Convert.ToDateTime(datefrom);// convert datefrom string to datetime variable 
                                                                             //if (to.Month >= DateTime.Now.Month && to.Day >= DateTime.Now.Day && from.Month <= DateTime.Now.Month && from.Day <= DateTime.Now.Day)
                                                                             //call alarm()
                                if (shefcode != "")
                                {
                                    alarm(ds.Tables[0].Rows[i]["Station_Full_Name"].ToString().Trim(), shefcode, ds.Tables[0].Rows[i]["SensorName"].ToString().Trim(), ds.Tables[0].Rows[i]["Sensor_Shef_Code"].ToString().Trim(), "Message: " + ds.Tables[0].Rows[i]["AlarmEmail"].ToString().Trim() + "\n", ds.Tables[0].Rows[i]["Deadband"].ToString().Trim(), ds.Tables[0].Rows[i]["ID"].ToString().Trim());
                                }
                            }
                            else
                            {
                                //call alarm()
                                if (shefcode != "")
                                {
                                    String qryOldFlag2 = "UPDATE public.\"tblAlarm2Sensor\" SET  \"EmailSent\"='true' WHERE \"Validity\"= 1 and \"AlarmType\"='single' and \"ID\"='"+ ds.Tables[0].Rows[i]["ID"] + "'";

                                    try
                                    {
                                        db.ExecuteQuery(qryOldFlag2, "", "", "");
                                    }
                                    catch (Exception e) { }

                                    alarm(ds.Tables[0].Rows[i]["Station_Full_Name"].ToString().Trim(), shefcode, ds.Tables[0].Rows[i]["SensorName"].ToString().Trim(), ds.Tables[0].Rows[i]["Sensor_Shef_Code"].ToString().Trim(), "Message: " + ds.Tables[0].Rows[i]["AlarmEmail"].ToString().Trim() + "\n", ds.Tables[0].Rows[i]["Deadband"].ToString().Trim(), ds.Tables[0].Rows[i]["ID"].ToString().Trim());
                                }
                            }


                       }
                    }
                }
                catch (Exception exc)
                {
                    WriteToFile(exc.Message);
                    //logfile.Flush();
                    WriteToFile(exc.StackTrace);
                    //logfile.Flush();

                }
                Thread.Sleep(6000);
            }

        }
        //sets the alarm flag to 11 
        public void setAlarmFlag211(String stnName, String sensor)//used for updating alarmflag cloumn values using stnName and sensor
        {
            WriteToFile("Entering setAlarmFlag211 for station" + stnName);
            //logfile.Flush();

            DateTime dNow = DateTime.Now;
            String year = dNow.Year.ToString().Substring(2, 2);
            //updating tblstation_name_year table alarmflag to 11
            String table = "'tblStation_" + stnName + "_" + year + "'";
            db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where \"AlarmFlag\"!='11' AND \"HydroMetParamsTypeId\"='" + sensor + "' AND make_timestamp(('20'||\"Year\"::text)::integer, \"Month\", \"Day\", \"Hour\", \"Minute\", \"Second\")  <= NOW()", "", "", "");

            WriteToFile("Setting Flag to 11 for these station =>" + table);
            //logfile.Flush();
            return;

        }
        /*
         Input parameter: val='value to convert from string to double'
         Returns Double value*/
        public double convert2Double(String val) //used to convert string to double value
        {
            try
            {
                return Convert.ToDouble(val.Trim());//trim the extra spcaes in the string and convert that to double by using predefined function 
            }
            catch (Exception exc)
            {
                return -1;// if error occur while converting then -1 is returned 
            }
        }
        //DateTime? RD_RateOfChange = null;
        DateTime? CD_RateOfChange = null;
        static Hashtable ROC_AlarmSent = new Hashtable();
        static Hashtable THRES_AlarmCond = new Hashtable();
        static Hashtable THRES_Email = new Hashtable();

        /*
         used to check alarm status 
         Input Parameters: ID, Logfile and type
         Return type: bool value*/
        public bool isLastAlarmOnThisStationAndSensor(String ID, StreamWriter logfile, String Type)
        {
            try
            {
                String checkQuery = "SELECT  a2s.\"ID\" FROM public.\"tblAlarm2Sensor\" a2s, (SELECT \"ID\", \"SensorName\", \"Station_Shef_Code\" FROM public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") t1 where a2s.\"Station_Shef_Code\" = t1.\"Station_Shef_Code\" and a2s.\"SensorName\" = t1.\"SensorName\" and a2s.\"ID\" > t1.\"ID\" and \"Validity\" = 1";
                DataSet ds_checkAlarm = db.getResultset(checkQuery, "", "", "");
                if (ds_checkAlarm == null)// checks if dataset is empty then returns true
                {
                    return true;
                }
                if (ds_checkAlarm.Tables[0].Rows.Count == 0) //checks if dataset first datatable is empty then returns true 
                {
                    return true;
                }
                if (ds_checkAlarm.Tables[0].Rows[0][0].ToString() == "0") //checks if datasets->datatable->datarow value is empty then returns true
                {
                    return true;
                }

                return false;
            }
            catch (Exception exc)
            {
                return false;
            }

        }
        Double mints = 0;
        /*
         used to construct mail body with required infromation */
        public void alarm(String stnfullname, String stnName, String sensorname, String sensor, String body, String deadband, String ID)
        {
            //logfile.Flush();

            DateTime dNow = DateTime.Now;
            String year = dNow.Year.ToString();
            String years = year.Substring(2, 2);

            String table = "'tblStation_" + stnName + "_" + years + "'";
            try
            {
                double values = 0;
                Boolean alarm = false;

                String sql = "select * from \"tblAlarm2Sensor\" where \"ID\"=" + ID;



                DataSet dsAlarm = db.getResultset(sql, "", "", "");

                if (dsAlarm == null)
                    return;


                String FY = "";//from year
                String FM = "";//from month
                String FD = "";//from date
                String h = "";//hour
                String m = "";//mintues
                String sec = "";//seconds
                String org_FY = "";
                String org_FM = "";
                String org_FD = "";
                String org_h = "";
                String org_m = "";
                String org_sec = "";
                double val = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Value"]);
                String sign = dsAlarm.Tables[0].Rows[0]["change"].ToString();
                String alarmName = dsAlarm.Tables[0].Rows[0]["AlarmName"].ToString();
                String type = dsAlarm.Tables[0].Rows[0]["Type"].ToString();
                //String time = dsAlarm.Tables[0].Rows[0]["Time"].ToString().Trim();

                DataSet sen_id_set = db.getResultset("select \"HydroMetParamsTypeId\" from \"tblHydroMetParamsType\" where \"HydroMetShefCode\"='" + sensor + "'", "", "", "");
                //String sql_query = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\" from \"'tblStation_" + stnName + "_18'\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\"  and a.\"HydroMetParamsTypeId\"='" + sen_id.Tables[0].Rows[0][0] + "'  and a.\"Flag\"= '1' order by \"HydroMetShefCode\",t asc  limit 1";

                String sen_id = sen_id_set.Tables[0].Rows[0][0].ToString().Trim();
                //String qryOldFlag = "UPDATE \"" + table + "\" SET \"AlarmFlag\"= 11 where \"HydroMetParamsTypeId\" = " + sen_id + " and \"Year\" <= (SELECT EXTRACT(YEAR FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"Month\"  <= (SELECT EXTRACT(MONTH FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"Day\"  <= (SELECT EXTRACT(DAY FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"Hour\"  <= (SELECT EXTRACT(HOUR FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"Minute\"  <= (SELECT EXTRACT(MINUTE FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"Second\"  <= (SELECT EXTRACT(SECOND FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ")  and \"AlarmFlag\"::integer != 11;";

                String qryOldFlag = "UPDATE \"" + table + "\" SET \"AlarmFlag\" = 11 where \"HydroMetParamsTypeId\" = " + sen_id + " and make_timestamp(('20'||\"Year\"::text)::integer, \"Month\", \"Day\", \"Hour\", \"Minute\", \"Second\")  <= (SELECT created_at from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"AlarmFlag\"::integer != 11;";

                try
                {
                    db.ExecuteQuery(qryOldFlag, "", "", "");
                }
                catch (Exception e) { }

                if (type == "Threshold_Value")
                {
                    // logfile.WriteLine("Threshold_Value");
                    //logfile.Flush();

                    String tablename = "'tblStation_" + stnName + "_" + years + "'";
                    if (sign.Trim() == ">")
                    {
                        DataSet ds = null;
                        string sql_query = "";
                        try
                        {
                            // logfile.WriteLine(">");
                            //logfile.Flush();

                            string sensortype = getSensorType(stnfullname, sensorname);

                            if (sensortype == "Real")
                            {
                                sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            }
                            else
                            {
                                sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"VirtualValue\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            }
                            //String sql_query = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\",b.\"HydroMetParamsTypeId\" from \"" + tablename + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\"  and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' order by \"HydroMetShefCode\",t asc  limit 1";

                            ds = db.getResultset(sql_query, "", "", "");
                            //logfile.Flush();

                            DateTime date = Convert.ToDateTime(ds.Tables[0].Rows[0][0].ToString());
                            Double mints = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Time"].ToString());
                            date = date.AddMinutes(-mints);

                            FD = date.Day.ToString();
                            FY = date.Year.ToString();
                            FM = date.Month.ToString();
                            h = date.Hour.ToString();
                            m = date.Minute.ToString();
                            org_FD = FD;
                            org_FY = FY;
                            org_FM = FM;
                            org_h = h;
                            org_m = m;

                            if (convert2Double(ds.Tables[0].Rows[0][2].ToString()) == -1)
                            {
                                //// logfile.WriteLine("Null value in alarm for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                //logfile.Flush();
                                return;
                            }

                            values = Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString());
                            WriteToFile(stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                            //logfile.Flush();

                            if ((values > val) && values != 0)
                            {
                                WriteToFile("Alarm Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                                //logfile.Flush();

                                THRES_AlarmCond[ID] = true;
                                alarm = true;
                                db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");

                            }
                            else
                            {
                                WriteToFile("Alarm Condition Not Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                                //logfile.Flush();

                            }

                            if (deadband.Trim() != "")
                            {

                                //Val = 13.0
                                //deadband = 0.2
                                //deadband_d = 13.0 - 0.2 = 12.8
                                Double deadband_d = val - Convert.ToDouble(deadband);

                                if (values < deadband_d)
                                {
                                    WriteToFile("Deadband Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                    //logfile.Flush();
                                    if (Convert.ToBoolean(DeadbandMet[ID]) == false)
                                        DeadbandMet[ID] = true;

                                    WriteToFile("Setting alarm flag to false  for stations " + stnName + " sensor name " + sen_id);
                                    //logfile.Flush();
                                    condition[sen_id + "" + stnName] = true;
                                }
                                else
                                {
                                    if (Convert.ToBoolean(DeadbandMet[ID]) == true)
                                    {
                                        WriteToFile("Deadband Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                        //logfile.Flush();
                                    }
                                    else
                                    {
                                        WriteToFile("Deadband Condition Not Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                        //logfile.Flush();

                                    }
                                }
                            }
                            else
                            {
                                condition[sen_id + "" + stnName] = true;

                            }
                        }
                        catch (Exception ex)
                        {
                            condition[sen_id + "" + stnName] = true;

                        }
                        try
                        {
                            if (isLastAlarmOnThisStationAndSensor(ID, logfile, "Threshold") == true)
                                db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds.Tables[0].Rows[0][3] + "'", "", "", "");
                        }
                        catch (Exception e) { }
                    }
                    if (sign.Trim() == "<")
                    {
                        DataSet ds = null;
                        string sql_query = "";
                        try
                        {
                            // logfile.WriteLine("<");
                            logfile.Flush();

                            string sensortype = getSensorType(stnfullname, sensorname);

                            if (sensortype == "Real")
                            {
                                sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            }
                            else
                            {
                                sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"VirtualValue\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            }
                            //String sql_query = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\",b.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\"  and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' order by \"HydroMetShefCode\",t asc  limit 1";

                            ds = db.getResultset(sql_query, "", "", "");
                            // logfile.WriteLine("No of rows =>" + ds.Tables[0].Rows.Count);
                            //logfile.Flush();

                            DateTime date = Convert.ToDateTime(ds.Tables[0].Rows[0][0].ToString());
                            Double mints = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Time"].ToString());
                            date = date.AddMinutes(-mints);
                            FD = date.Day.ToString();
                            FY = date.Year.ToString();
                            FM = date.Month.ToString();
                            h = date.Hour.ToString();
                            m = date.Minute.ToString();
                            org_FD = FD;
                            org_FY = FY;
                            org_FM = FM;
                            org_h = h;
                            org_m = m;
                            if (convert2Double(ds.Tables[0].Rows[0][2].ToString()) == -1)
                            {
                                // logfile.WriteLine("Null value in alarm for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                //logfile.Flush();
                                return;
                            }

                            values = Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString());
                            WriteToFile(stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                            logfile.Flush();

                            if (values < val)
                            {
                                WriteToFile("Alarm Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                                logfile.Flush();

                                THRES_AlarmCond[ID] = true;
                                alarm = true;
                                db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");

                            }
                            else
                            {
                                WriteToFile("Alarm Condition Not Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                                logfile.Flush();

                            }
                            if (deadband.Trim() != "")
                            {


                                Double deadband_d = val + Convert.ToDouble(deadband);

                                if (values > deadband_d)
                                {
                                    WriteToFile("Deadband Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                    //logfile.Flush();
                                    DeadbandMet[ID] = true;
                                    WriteToFile("Setting alarm flag to false  for stations " + stnName + " sensor name " + sen_id);
                                    //logfile.Flush();
                                    condition[sen_id + "" + stnName] = true;
                                }
                                else
                                {
                                    if (Convert.ToBoolean(DeadbandMet[ID]) == true)
                                    {
                                        WriteToFile("Deadband Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                        //logfile.Flush();
                                    }
                                    else
                                    {
                                        WriteToFile("Deadband Condition Not Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                        //logfile.Flush();

                                    }
                                }
                            }
                            else
                            {
                                condition[sen_id + "" + stnName] = true;

                            }
                        }
                        catch (Exception ex)
                        {
                            condition[sen_id + "" + stnName] = true;

                        }
                        try
                        {
                            if (isLastAlarmOnThisStationAndSensor(ID, logfile, "Threshold") == true)
                                db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds.Tables[0].Rows[0][3] + "'", "", "", "");
                        }
                        catch (Exception e) { }
                    }
                    if (sign.Trim() == "<=")
                    {
                        DataSet ds = null;
                        try
                        {
                            // logfile.WriteLine("<=");
                            logfile.Flush();

                            string sensortype = getSensorType(stnfullname, sensorname);

                            if (sensortype == "Real")
                            { }
                            else { }
                            //String sql_query = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\",b.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\"  and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' order by \"HydroMetShefCode\",t asc  limit 1";
                            String sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            ds = db.getResultset(sql_query, "", "", "");
                            // logfile.WriteLine("No of rows =>" + ds.Tables[0].Rows.Count);
                            //logfile.Flush();

                            DateTime date = Convert.ToDateTime(ds.Tables[0].Rows[0][0].ToString());
                            Double mints = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Time"].ToString());
                            date = date.AddMinutes(-mints);
                            FD = date.Day.ToString();
                            FY = date.Year.ToString();
                            FM = date.Month.ToString();
                            h = date.Hour.ToString();
                            m = date.Minute.ToString();
                            org_FD = FD;
                            org_FY = FY;
                            org_FM = FM;
                            org_h = h;
                            org_m = m;

                            if (convert2Double(ds.Tables[0].Rows[0][2].ToString()) == -1)
                            {
                                WriteToFile("Null value in alarm for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                //logfile.Flush();
                                return;
                            }

                            values = Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString());

                            if ((values <= val) && values != 0)
                            {
                                THRES_AlarmCond[ID] = true;
                                alarm = true;
                                db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");

                            }
                            if (deadband.Trim() != "")
                            {


                                Double deadband_d = val + Convert.ToDouble(deadband);

                                if (values >= deadband_d)
                                {
                                    WriteToFile("Values = " + values + " is less than deathband = " + deadband_d + " for stations " + stnName + " sensor name " + sen_id + " YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                    //logfile.Flush();
                                    DeadbandMet[ID] = true;
                                    WriteToFile("Setting alarm flag to false  for stations " + stnName + " sensor name " + sen_id);
                                    //logfile.Flush();
                                    condition[sen_id + "" + stnName] = true;
                                }
                            }
                            else
                            {
                                condition[sen_id + "" + stnName] = true;

                            }
                        }
                        catch (Exception ex)
                        {
                            condition[sen_id + "" + stnName] = true;

                        }
                        try
                        {
                            if (isLastAlarmOnThisStationAndSensor(ID, logfile, "Threshold") == true)
                                db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds.Tables[0].Rows[0][3] + "'", "", "", "");
                        }
                        catch (Exception e) { }
                    }
                    if (sign.Trim() == ">=")
                    {
                        DataSet ds = null;
                        string sql_query = "";
                        try
                        {
                            // logfile.WriteLine(">=");
                            logfile.Flush();
                            string sensortype = getSensorType(stnfullname, sensorname);

                            if (sensortype == "Real")
                            {
                                sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            }
                            else
                            {
                                sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"VirtualValue\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            }
                            //  String sql_query = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\",b.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\"  and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' order by \"HydroMetShefCode\",t asc  limit 1";

                            ds = db.getResultset(sql_query, "", "", "");
                            // logfile.WriteLine("No of rows =>" + ds.Tables[0].Rows.Count);
                            //logfile.Flush();

                            DateTime date = Convert.ToDateTime(ds.Tables[0].Rows[0][0].ToString());
                            Double mints = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Time"].ToString());
                            date = date.AddMinutes(-mints);
                            FD = date.Day.ToString();
                            FY = date.Year.ToString();
                            FM = date.Month.ToString();
                            h = date.Hour.ToString();
                            m = date.Minute.ToString();
                            org_FD = FD;
                            org_FY = FY;
                            org_FM = FM;
                            org_h = h;
                            org_m = m;

                            if (convert2Double(ds.Tables[0].Rows[0][2].ToString()) == -1)
                            {
                                WriteToFile("Null value in alarm for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                //logfile.Flush();
                                return;
                            }

                            values = Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString());

                            if ((values >= val) && values != 0)
                            {
                                THRES_AlarmCond[ID] = true;
                                alarm = true;
                                db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");

                            }
                            if (deadband.Trim() != "")
                            {


                                Double deadband_d = val - Convert.ToDouble(deadband);

                                if (values <= deadband_d)
                                {
                                    WriteToFile("Values = " + values + " is less than deathband = " + deadband_d + " for stations " + stnName + " sensor name " + sen_id + " YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                    //logfile.Flush();
                                    DeadbandMet[ID] = true;
                                    WriteToFile("Setting alarm flag to false  for stations " + stnName + " sensor name " + sen_id);
                                    //logfile.Flush();
                                    condition[sen_id + "" + stnName] = true;
                                }
                            }
                            else
                            {
                                condition[sen_id + "" + stnName] = true;

                            }
                        }
                        catch (Exception ex)
                        {
                            condition[sen_id + "" + stnName] = true;

                        }
                        try
                        {
                            if (isLastAlarmOnThisStationAndSensor(ID, logfile, "Threshold") == true)
                                db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds.Tables[0].Rows[0][3] + "'", "", "", "");
                        }
                        catch (Exception e) { }
                    }
                }
                else if (type == "Rate_of_Change")
                {

                    // logfile.WriteLine("Rate_of_Change");
                    logfile.Flush();

                    if (sign.Trim() == "+")
                    {
                        DataSet ds2 = null;
                        string sql_query2 = "";
                        try
                        {
                            // logfile.WriteLine("+");
                            logfile.Flush();
                            // String sql_query2 = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\",b.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' order by \"HydroMetShefCode\",t limit 1";
                            //  String pv = "SELECT \"VirtualValue\" as \"Value\" FROM public.\"tblAlarm2Sensor\" where created_at<now() order by \"created_at\" desc  limit 1";
                            string sensortype = getSensorType(stnfullname, sensorname);

                            if (sensortype == "Real")
                            {
                                sql_query2 = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            }
                            else
                            {
                                sql_query2 = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"VirtualValue\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            }

                            ds2 = db.getResultset(sql_query2, "", "", "");
                            // logfile.WriteLine("No of rows =>" + ds2.Tables[0].Rows.Count);
                            logfile.Flush();

                            DateTime date = Convert.ToDateTime(ds2.Tables[0].Rows[0][0].ToString());
                            int HydroMetParamsTypeId = Convert.ToInt32(sen_id);
                            //string  HydroMetShefCode = (dsAlarm.Tables[0].Rows[0]["HydroMetShefCode"].ToString());
                            FD = date.Day.ToString();
                            FY = date.Year.ToString().Substring(2, 2);
                            FM = date.Month.ToString();
                            h = date.Hour.ToString();
                            m = date.Minute.ToString();
                            sec = date.Second.ToString();

                            org_FD = FD;
                            org_FY = FY;
                            org_FM = FM;
                            org_h = h;
                            org_m = m;


                            int hh = date.Hour;
                            int MM = date.Minute;
                            int ss = date.Second;
                            int SpanTime = Convert.ToInt32(dsAlarm.Tables[0].Rows[0]["Time"]);

                            DateTime Pdate = date.AddMinutes(-SpanTime);

                            //TimeSpan ts = new TimeSpan(hh, MM, ss);
                            //TimeSpan PDate = ts.Subtract(new TimeSpan(0, SpanTime, 0));


                            int hr = Pdate.Hour;
                            int mn = Pdate.Minute;
                            int sc = Pdate.Second;
                            FD = Pdate.Day.ToString();
                            FY = Pdate.Year.ToString().Substring(2, 2);
                            FM = Pdate.Month.ToString();
                            //  string Pvalue = "SELECt Value FROM 'tblStation_SF14_18' where Year=18 and Month=1 and Day=1 and Hour=0 and Minute=30 and Second=0 AND AlarmFlag=0 ANd HydroMetParamsTypeId=167 ";

                            //" + sen_id + "=167
                            //" + mn + "=30
                            string Pvalue = "SELECt \"Value\" as \"Value\",\"AlarmFlag\" from public.\"" + table + "\"  where \"Year\"=" + FY + " and \"Month\"=" + FM + " and \"Day\"=" + FD + " and \"Hour\"=" + hr + " and \"Minute\"=" + mn + " and \"Second\"=" + sec + " ANd \"HydroMetParamsTypeId\"=" + sen_id + " ";
                            //string Pvalue = "SELECt \"VirtualValue\" as \"Value\" FROM public.\"'tblStation_SF14_18'\" where \"Year\"=18 and \"Month\"=1 and \"Day\"=1 and \"Hour\"=0 and \"Minute\"=30 and \"Second\"=0 AND \"AlarmFlag\"='0' ANd \"HydroMetParamsTypeId\"=167 ";
                            //  string Pvalue = "SELECT \"VirtualValue\" as \"Value\" FROM public.\"'tblStation_SF10_18'\" where \"Year\"=18 and \"Month\"=1 and \"Day\"=1 and \"Hour\"=1 and \"Minute\"=0 and \"Second\"=0";
                            //string Pvalue = "SELECt \"VirtualValue\" as \"Value\" from \"" + table + "\" a where \"Year\"=" + FY + " and \"Month\"=" + FM+ " and \"Day\"=" + FD + " and \"Hour\"=" + hr + " and \"Minute\"=" + mn + " and \"Second\"=" + sc + " AND \"AlarmFlag\"='0' ANd \"HydroMetParamsTypeId\"=" + sen_id + "";
                            // string Pvalue = "SELECt \"VirtualValue\" as \"Value\" FROM public.\'" + table + "\'";
                            // string Pvalue = "SELECT \"VirtualValue\" as \"Value\" FROM public.\"'"+table+"'\" where \"Year\"=18 and \"Month\"=1 and \"Day\"=1 and \"Hour\"=6 and \"Minute\"=45 and \"Second\"=0";

                            //  Double PDate  = Convert.ToDouble(date.AddMinutes(-mints));
                            // string Pvalue = "Select \"VirtualValue\" as \"Value\"   from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' and \"Hour\"=" + hr + " and \"Minute\"=" + mn + " and \"Second\"=" + sec + " and \"Year\"=" + FY + " and \"Month\"=" + FM + " and \"Day\"=" + FD + "";
                            //   string Pvalue = "Select \"VirtualValue\" as \"Value\"   from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' and \"Hour\"='" + hr + "' and \"Minute\"='" + mn + "' and \"Second\"='" + sec + "' and \"Year\"='"+FY+"' and \"Month\"='"+FM+"' and \"Day\"='"+FD+"'";
                            // string Pvalue = "Select Value  from \"" + table + "\" a, where Change=" + sign+" and AlarmName="+alarmName+" and Type="+type+" and Hour="+hr+" and Minute="+mn+" and Second="+sec+"";
                            // string Sp = "select   \"Time\", from \"tblAlarm2Sensor\" where \"created_at\"="+date+"";
                            // string Sp = "SELECT  \"Time\" FROM public.\""+table+"\" where \"VirtualValue\" as \"Value\"=40";
                            // int i = Convert.ToInt32(cmd.ExecuteScalar());
                            DataSet dPrevValue = db.getResultset(Pvalue, "", "", "");

                            if (dPrevValue != null && dPrevValue.Tables[0].Rows.Count > 0)
                            {
                                Double PreviousValue = Convert.ToDouble(dPrevValue.Tables[0].Rows[0][0]);
                                Double CurrentValue = Convert.ToDouble(ds2.Tables[0].Rows[0][2].ToString().Trim());
                                values = CurrentValue;
                                double AP = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Value"]);

                                // logfile.WriteLine("PreviousValue = " + PreviousValue + "; CurrentValue = " + CurrentValue + "; AP = " + AP);
                                logfile.Flush();

                                //Double PreviousValue = 15.3;
                                //Double AP = 0.1;
                                //Double CurrentValue = 16.3;
                                Double ROCT = PreviousValue + AP;
                                if (CurrentValue >= ROCT)
                                {
                                    alarm = true;
                                    CD_RateOfChange = date;

                                    // db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds2.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds2.Tables[0].Rows[0][3] + "'", "", "", "");
                                    db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");
                                }
                                else
                                {
                                    alarm = false;
                                }
                                // string PreviousValue = "Select Value from "+table+" where Minutes="+PDate+" ";
                                ////m = date.Minute.ToString();
                                // String str1 = ds2.Tables[0].Rows[0][2].ToString().Trim();


                                //Double v1 = val + Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString().Trim());
                            }


                        }
                        catch (Exception exc)
                        { }
                        try
                        {
                            if (isLastAlarmOnThisStationAndSensor(ID, logfile, "ROC") == true)
                            {
                                DateTime date = Convert.ToDateTime(ds2.Tables[0].Rows[0][0].ToString());
                                FD = date.Day.ToString();
                                FY = date.Year.ToString().Substring(2, 2);
                                FM = date.Month.ToString();
                                h = date.Hour.ToString();
                                m = date.Minute.ToString();
                                sec = date.Second.ToString();

                                string AlarmTrue = "Update public.\"" + table + "\" set \"AlarmFlag\"='11' Where \"Year\"=" + FY + " and \"Month\"=" + FM + " and \"Day\"=" + FD + " and \"Hour\"=" + h + " and \"Minute\"=" + m + " and \"Second\"=" + sec + " ANd \"HydroMetParamsTypeId\"=" + sen_id + " ";
                                db.ExecuteQuery(AlarmTrue, "", "", "");
                            }
                        }
                        catch (Exception e) { }
                        Double v2 = val + Convert.ToDouble(ds2.Tables[0].Rows[0][2].ToString().Trim());
                    }
                    if (sign.Trim() == "-")
                    {
                        DataSet ds1 = null;
                        string sql_query1 = "";
                        try
                        {
                            // logfile.WriteLine("-");
                            logfile.Flush();
                            string sensortype = getSensorType(stnfullname, sensorname);

                            if (sensortype == "Real")
                            {
                                sql_query1 = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            }
                            else
                            {
                                sql_query1 = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"VirtualValue\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                            }

                            //String pv = "SELECT \"VirtualValue\" as \"Value\" FROM public.\"tblAlarm2Sensor\" where created_at<now() order by \"created_at\" desc  limit 1";

                            ds1 = db.getResultset(sql_query1, "", "", "");
                            // logfile.WriteLine("No of rows =>" + ds1.Tables[0].Rows.Count);

                            logfile.Flush(); String strDate = ds1.Tables[0].Rows[0][0].ToString();
                            DateTime date = Convert.ToDateTime(strDate);

                            FD = date.Day.ToString();
                            FY = date.Year.ToString().Substring(2, 2);
                            FM = date.Month.ToString();
                            h = date.Hour.ToString();
                            m = date.Minute.ToString();

                            org_FD = FD;
                            org_FY = FY;
                            org_FM = FM;
                            org_h = h;
                            org_m = m;


                            h = date.Hour.ToString();
                            m = date.Minute.ToString();
                            sec = date.Second.ToString();


                            int hh = date.Hour;
                            int MM = date.Minute;
                            int ss = date.Second;
                            int SpanTime = Convert.ToInt32(dsAlarm.Tables[0].Rows[0]["Time"]);


                            DateTime Pdate = date.AddMinutes(-SpanTime);

                            //TimeSpan ts = new TimeSpan(hh, MM, ss);
                            //TimeSpan PDate = ts.Subtract(new TimeSpan(0, SpanTime, 0));


                            int hr = Pdate.Hour;
                            int mn = Pdate.Minute;
                            int sc = Pdate.Second;
                            FD = Pdate.Day.ToString();
                            FY = Pdate.Year.ToString().Substring(2, 2);
                            FM = Pdate.Month.ToString();


                            string Pvalue1 = "SELECt \"Value\" as \"Value\",\"AlarmFlag\" from public.\"" + table + "\"  where \"Year\"=" + FY + " and \"Month\"=" + FM + " and \"Day\"=" + FD + " and \"Hour\"=" + hr + " and \"Minute\"=" + mn + " and \"Second\"=" + sec + " ANd \"HydroMetParamsTypeId\"=" + sen_id + " ";
                            DataSet dPrevValue = db.getResultset(Pvalue1, "", "", "");
                            if (dPrevValue != null && dPrevValue.Tables[0].Rows.Count > 0)
                            {
                                Double PreviousValue1 = Convert.ToDouble(dPrevValue.Tables[0].Rows[0][0]);
                                Double CurrentValue1 = Convert.ToDouble(ds1.Tables[0].Rows[0][2].ToString().Trim());
                                values = CurrentValue1;
                                double AP = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Value"]);
                                //Double PreviousValue = 11.3;
                                //Double AP = 0.5;
                                //Double CurrentValue = 9.3;
                                Double ROCT = PreviousValue1 - AP;
                                if (CurrentValue1 <= ROCT)
                                {
                                    alarm = true;
                                    CD_RateOfChange = date;

                                    // db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds2.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds2.Tables[0].Rows[0][3] + "'", "", "", "");
                                    db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");
                                }
                                else
                                {
                                    alarm = false;
                                }
                            }

                        }
                        catch (Exception exc)
                        { }
                        try
                        {
                            if (isLastAlarmOnThisStationAndSensor(ID, logfile, "ROC") == true)
                            {
                                String strDate = ds1.Tables[0].Rows[0][0].ToString();
                                DateTime date = Convert.ToDateTime(strDate);

                                FD = date.Day.ToString();
                                FY = date.Year.ToString().Substring(2, 2);
                                FM = date.Month.ToString();
                                h = date.Hour.ToString();
                                m = date.Minute.ToString();
                                org_FD = FD;
                                org_FY = FY;
                                org_FM = FM;
                                org_h = h;
                                org_m = m;

                                string AlarmTrue = "Update public.\"" + table + "\" set \"AlarmFlag\"='11' Where \"Year\"=" + FY + " and \"Month\"=" + FM + " and \"Day\"=" + FD + " and \"Hour\"=" + h + " and \"Minute\"=" + m + " and \"Second\"=" + sec + " ANd \"HydroMetParamsTypeId\"=" + sen_id + " ";
                                db.ExecuteQuery(AlarmTrue, "", "", "");
                            }
                        }
                        catch (Exception e) { }
                    }

                }
                else
                {
                    WriteToFile(type + " Does not exist");
                    logfile.Flush();
                }

                if (alarm)
                {
                    List<string> lstMailIds = new List<string>();
                    DataSet dsAlarmUser = db.getResultset("select \"communicationway\",\"Username\" from \"tblAlarmType2User\" where \"AlarmName\"='" + alarmName + "'", "", "", "");
                    if (dsAlarmUser == null)
                        return;
                    System.Net.Mail.MailMessage mail = new System.Net.Mail.MailMessage();

                    for (int i = 0; i < dsAlarmUser.Tables[0].Rows.Count; i++)
                    {

                        string usersvalues = dsAlarmUser.Tables[0].Rows[i]["Username"].ToString();
                        string substr1 = "Group - ";
                        if (usersvalues.Contains(substr1))
                        {
                            DataSet dsAlarmUsernew = db.getResultset("select \"GroupCommunicationWay\" from \"tblloginandregister\"", "", "", "");
                            if (dsAlarmUsernew.Tables[0].Rows[i]["GroupCommunicationWay"].ToString().Trim() != "" || dsAlarmUsernew.Tables[0].Rows[i]["GroupCommunicationWay"].ToString().Trim() != null)
                            {
                                string checkusergroup = usersvalues.Replace("Group - ", "").ToString();
                                DataSet dsemailed = db.getResultset("select \"Email\",\"mobileno\",\"GroupName\",\"GroupCommunicationWay\" from \"tblloginandregister\"", "", "", "");
                                for (int k = 0; k < dsemailed.Tables[0].Rows.Count; k++)
                                {
                                    string temp = dsemailed.Tables[0].Rows[k]["GroupName"].ToString().Trim();
                                    string tempway = dsemailed.Tables[0].Rows[k]["GroupCommunicationWay"].ToString().Trim();
                                    char[] spearator = { ',' };
                                    string[] groupusers = temp.Split(spearator);
                                    string[] groupusersway = tempway.Split(spearator);

                                    for (int q = 0; q < groupusers.Length; q++)
                                    //foreach (string usergroups in groupusers)
                                    {
                                        if (groupusers[q] == checkusergroup)
                                        {
                                            if (groupusersway[q].ToString().Trim() == "Email" || groupusersway[q].ToString().Trim() == "Both")
                                            {
                                                if (dsemailed.Tables[0].Rows[k]["Email"].ToString().Trim() != null)
                                                {
                                                    mail.To.Add(dsemailed.Tables[0].Rows[k]["Email"].ToString().Trim());
                                                    WriteToFile("Alarm ID =" + ID + " EMail Added to Alarm: " + dsemailed.Tables[0].Rows[k]["Email"].ToString().Trim());
                                                    mail.To.Add(dsemailed.Tables[0].Rows[k]["mobileno"].ToString().Trim() + "@tmomail.net");
                                                    WriteToFile("Alarm ID =" + ID + " Mobile Added to Alarm: " + dsemailed.Tables[0].Rows[k]["mobileno"].ToString().Trim()
                                                       );
                                                    lstMailIds.Add(dsemailed.Tables[0].Rows[k]["Email"].ToString().Trim());

                                                }
                                                else
                                                {
                                                    WriteToFile("Alarm ID =" + ID + " No email id and number is available");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else

                        {
                            if (dsAlarmUser.Tables[0].Rows[i]["communicationway"].ToString().Trim() == "Email" || dsAlarmUser.Tables[0].Rows[i]["communicationway"].ToString().Trim() == "Both")
                            {
                                DataSet dsemail = db.getResultset("select \"Email\",\"mobileno\" from \"tblloginandregister\" where \"Username\"='" + usersvalues + "'", "", "", "");
                                if (dsemail != null)
                                {
                                    mail.To.Add(dsemail.Tables[0].Rows[0]["Email"].ToString().Trim());
                                    WriteToFile("Alarm ID =" + ID + " EMail Added to Alarm: " + dsemail.Tables[0].Rows[0]["Email"].ToString().Trim());
                                    mail.To.Add(dsemail.Tables[0].Rows[0]["mobileno"].ToString().Trim() + "@tmomail.net");
                                    WriteToFile("Alarm ID =" + ID + " Mobile Added to Alarm: " + dsemail.Tables[0].Rows[0]["mobileno"].ToString().Trim());
                                }
                                else
                                {
                                    WriteToFile("Alarm ID =" + ID + " No email id and number is available");
                                }
                            }
                        }
                    }

                    if (type == "Threshold_Value")//if type =Threshold value and change = ">"
                    {

                        if (Convert.ToBoolean(THRES_AlarmCond[ID]) == true)
                        {

                            // First time case
                            if (Convert.ToBoolean(THRES_Email[ID]) == false)//false
                            {
                                WriteToFile("Sending email for first time and not checking deathband with body " + body + " for stations " + stnName + " sensor name " + sen_id + " value " + values);
                                WriteToFile("Sending email with body " + body + " for stations " + stnName + " sensor name " + sen_id + " value " + values);
                                logfile.Flush();
                                //String stnFullName = db.getstationname(stnName);
                                //String senFullName = db.getsensorname(stnfullname, stnName, sensor);
                                body += ("Station Name: " + stnfullname + "\n" + "Sensor Name: " + sensorname + "\n" + "Current Value:" + values + "\n" + "Time Stamp: " + org_FY + "-" + Convert.ToInt32(org_FM).ToString("00") + "-" + Convert.ToInt32(org_FD).ToString("00") + " " + Convert.ToInt32(org_h).ToString("00") + ":" + Convert.ToInt32(org_m).ToString("00"));
                                email(mail, body);
                                //SendMail(lstMailIds, body);
                                THRES_Email[ID] = true; // Alarm is sent so flagging as true so that email is not sent second time
                                THRES_AlarmCond[ID] = false;
                                DeadbandMet[ID] = false;
                                WriteToFile("Setting alarm flag to true  for stations " + stnName + " sensor name " + sen_id);
                                logfile.Flush();

                            }
                            else
                            {
                                // not first time checking dead band
                                if (Convert.ToBoolean(DeadbandMet[ID]) == true)//true
                                {

                                    WriteToFile("Sending email with body " + body + " for stations " + stnName + " sensor name " + sen_id + " value " + values);
                                    logfile.Flush();
                                    WriteToFile("2ndR");
                                    String stnFullName = db.getstationname(stnName);//gets station fullname
                                    String senFullName = db.getsensorname(stnFullName, stnName, sensor);//gets station sensorname 
                                    body += ("Station Name: " + stnFullName + "\n" + "Sensor Name: " + senFullName + "\n" + "Current Value:" + values + "\n" + "Time Stamp: " + org_FY + "-" + Convert.ToInt32(org_FM).ToString("00") + "-" + Convert.ToInt32(org_FD).ToString("00") + " " + Convert.ToInt32(org_h).ToString("00") + ":" + Convert.ToInt32(org_m).ToString("00"));
                                    //creates body of the mail 
                                    email(mail, body);// call email() function tosend email to user 
                                    //SendMail(lstMailIds, body);
                                    THRES_Email[ID] = true;
                                    DeadbandMet[ID] = false;
                                    THRES_AlarmCond[ID] = false;

                                    WriteToFile("Setting alarm flag to true  for stations " + stnName + " sensor name " + sen_id);
                                    logfile.Flush();

                                }
                                else
                                {
                                    WriteToFile("Ignoring alarm  for stations " + stnName + " sensor name " + sen_id + " value " + values);
                                    logfile.Flush();

                                }

                            }
                        }


                    }
                    else
                    {
                        DateTime? dt_alarmSent = null;

                        if (ROC_AlarmSent[ID] != null)
                        {
                            dt_alarmSent = (DateTime)ROC_AlarmSent[ID];
                        }
                        if (ROC_AlarmSent[ID] == null)
                        {
                            WriteToFile("Sending email with body " + body + " for stations " + stnName + " sensor name " + sen_id + " value " + values);
                            logfile.Flush();
                            WriteToFile("3rdR");
                            String stnFullName = db.getstationname(stnName);//gets station fullname 
                            String senFullName = db.getsensorname(stnFullName, stnName, sensor);//gets station sensorname 
                            body += ("Station Name: " + stnFullName + "\n" + "Sensor Name: " + senFullName + "\n" + "Current Value:" + values + "\n" + "Time Stamp: " + org_FY + ":" + Convert.ToInt32(org_FM).ToString("00") + ":" + Convert.ToInt32(org_FD).ToString("00") + " " + Convert.ToInt32(org_h).ToString("00") + ":" + Convert.ToInt32(org_m).ToString("00"));
                            //creates body of the mail 
                            email(mail, body);// call email() function tosend email to user  
                            //SendMail(lstMailIds, body);
                            ROC_AlarmSent[ID] = CD_RateOfChange;


                            WriteToFile("Setting alarm flag to true  for stations " + stnName + " sensor name " + sen_id);
                            logfile.Flush();
                        }
                        else if (CD_RateOfChange.Value.Subtract(dt_alarmSent.Value) >= TimeSpan.FromMinutes(mints))
                        {
                            TimeSpan bal_time = CD_RateOfChange.Value.Subtract(dt_alarmSent.Value);
                            WriteToFile("Sending email with body " + body + " for stations " + stnName + " sensor name " + sen_id + " value " + values + " and balance time span is " + bal_time);
                            logfile.Flush();
                            String stnFullName = db.getstationname(stnName);//gets station fullname 
                            String senFullName = db.getsensorname(stnFullName, stnName, sensor);//gets station sensorname 
                            body += ("Station Name: " + stnFullName + "\n" + "Sensor Name: " + senFullName + "\n" + "Current Value:" + values + "\n" + "Time Stamp: " + org_FY + "-" + Convert.ToInt32(org_FM).ToString("00") + "-" + Convert.ToInt32(org_FD).ToString("00") + " " + Convert.ToInt32(org_h).ToString("00") + ":" + Convert.ToInt32(org_m).ToString("00"));
                            //creates body of the mail 
                            email(mail, body);// call email() function tosend email to user 
                                              // SendMail(lstMailIds, body);
                            ROC_AlarmSent[ID] = CD_RateOfChange;


                            WriteToFile("Setting alarm flag to true  for stations " + stnName + " sensor name " + sen_id);
                            logfile.Flush();
                        }
                        else
                        {


                            TimeSpan bal_time = CD_RateOfChange.Value.Subtract(dt_alarmSent.Value);
                            WriteToFile("Ignoring alarm  for stations " + stnName + " sensor name " + sen_id + " value " + values + " because timespan is " + bal_time.Minutes + " at " + CD_RateOfChange.Value);
                            logfile.Flush();

                        }
                    }
                    if (deadband.Trim() != "")
                        condition[sen_id + "" + stnName] = false;

                }
                else
                {
                    //update alarmflag to false where StationShefCode =stnName
                    db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='false' where \"StationShefCode\"='" + stnName + "'", "", "", "");

                }
                //db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='1' where \"AlarmFlag\"!='11'", "", "", "");
                //set alarm to false 
                alarm = false;

            }
            catch (Exception ex)
            {
                WriteToFile("Exception " + ex.Message);
                logfile.Flush();
            }
        }


        public void multialarm(DataTable dt)
        {
            Boolean flag = true;
            List<string> lstMailIds = new List<string>();
            System.Net.Mail.MailMessage mail = new System.Net.Mail.MailMessage();
            DateTime dNow = DateTime.Now;
            String year = dNow.Year.ToString();
            String years = year.Substring(2, 2);


            String body = "";
            String stnfullname = "";
            String stnName = "";
            String sensorname = "";
            String sensor = "";
            String deadband = "";
            String ID = "";
            String sen_id = "";
            String combinedBody = "";
            int tosentmail = 0;
            for (int x = 0; x < dt.Rows.Count; x++)
            {
                if (dt.Rows[x]["AlarmType"].ToString() == "multiple")
                {
                    stnfullname = dt.Rows[x]["Station_Full_Name"].ToString().Trim();
                    stnName = dt.Rows[x]["Station_Shef_Code"].ToString().Trim();
                    sensorname = dt.Rows[x]["SensorName"].ToString().Trim();
                    sensor = dt.Rows[x]["Sensor_Shef_Code"].ToString().Trim();
                    body = "Message: " + dt.Rows[x]["AlarmEmail"].ToString().Trim() + "\n";
                    deadband = dt.Rows[x]["Deadband"].ToString().Trim();
                    ID = dt.Rows[x]["ID"].ToString().Trim();

                    String table = "'tblStation_" + stnName + "_" + years + "'";
                    try
                    {
                        double values = 0;
                        Boolean alarm = false;

                        String sql = "select * from \"tblAlarm2Sensor\" where \"ID\"=" + ID;



                        DataSet dsAlarm = db.getResultset(sql, "", "", "");

                        if (dsAlarm == null)
                            return;


                        String FY = "";//from year
                        String FM = "";//from month
                        String FD = "";//from date
                        String h = "";//hour
                        String m = "";//mintues
                        String sec = "";//seconds
                        String org_FY = "";
                        String org_FM = "";
                        String org_FD = "";
                        String org_h = "";
                        String org_m = "";
                        String org_sec = "";
                        double val = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Value"]);
                        String sign = dsAlarm.Tables[0].Rows[0]["change"].ToString();
                        String alarmName = dsAlarm.Tables[0].Rows[0]["AlarmName"].ToString();
                        String type = dsAlarm.Tables[0].Rows[0]["Type"].ToString();
                        //String time = dsAlarm.Tables[0].Rows[0]["Time"].ToString().Trim();

                        DataSet sen_id_set = db.getResultset("select \"HydroMetParamsTypeId\" from \"tblHydroMetParamsType\" where \"HydroMetShefCode\"='" + sensor + "'", "", "", "");
                        //String sql_query = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\" from \"'tblStation_" + stnName + "_18'\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\"  and a.\"HydroMetParamsTypeId\"='" + sen_id.Tables[0].Rows[0][0] + "'  and a.\"Flag\"= '1' order by \"HydroMetShefCode\",t asc  limit 1";

                        sen_id = sen_id_set.Tables[0].Rows[0][0].ToString().Trim();
                        //String qryOldFlag = "UPDATE \"" + table + "\" SET \"AlarmFlag\"= 11 where \"HydroMetParamsTypeId\" = " + sen_id + " and \"Year\" <= (SELECT EXTRACT(YEAR FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"Month\"  <= (SELECT EXTRACT(MONTH FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"Day\"  <= (SELECT EXTRACT(DAY FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"Hour\"  <= (SELECT EXTRACT(HOUR FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"Minute\"  <= (SELECT EXTRACT(MINUTE FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"Second\"  <= (SELECT EXTRACT(SECOND FROM created_at) from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ")  and \"AlarmFlag\"::integer != 11;";

                        String qryOldFlag = "UPDATE \"" + table + "\" SET \"AlarmFlag\" = 11 where \"HydroMetParamsTypeId\" = " + sen_id + " and make_timestamp(('20'||\"Year\"::text)::integer, \"Month\", \"Day\", \"Hour\", \"Minute\", \"Second\")  <= (SELECT created_at from public.\"tblAlarm2Sensor\" where \"ID\" = " + ID + ") and \"AlarmFlag\"::integer != 11;";

                        try
                        {
                            db.ExecuteQuery(qryOldFlag, "", "", "");
                        }
                        catch (Exception e) { }

                        if (type == "Threshold_Value")
                        {
                            // logfile.WriteLine("Threshold_Value");
                            //logfile.Flush();

                            String tablename = "'tblStation_" + stnName + "_" + years + "'";
                            if (sign.Trim() == ">")
                            {
                                DataSet ds = null;
                                string sql_query = "";
                                try
                                {
                                    // logfile.WriteLine(">");
                                    //logfile.Flush();

                                    string sensortype = getSensorType(stnfullname, sensorname);

                                    if (sensortype == "Real")
                                    {
                                        sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    }
                                    else
                                    {
                                        sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"VirtualValue\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    }
                                    //String sql_query = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\",b.\"HydroMetParamsTypeId\" from \"" + tablename + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\"  and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' order by \"HydroMetShefCode\",t asc  limit 1";

                                    ds = db.getResultset(sql_query, "", "", "");
                                    //logfile.Flush();

                                    DateTime date = Convert.ToDateTime(ds.Tables[0].Rows[0][0].ToString());
                                    Double mints = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Time"].ToString());
                                    date = date.AddMinutes(-mints);

                                    FD = date.Day.ToString();
                                    FY = date.Year.ToString();
                                    FM = date.Month.ToString();
                                    h = date.Hour.ToString();
                                    m = date.Minute.ToString();
                                    org_FD = FD;
                                    org_FY = FY;
                                    org_FM = FM;
                                    org_h = h;
                                    org_m = m;

                                    if (convert2Double(ds.Tables[0].Rows[0][2].ToString()) == -1)
                                    {
                                        //// logfile.WriteLine("Null value in alarm for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                        //logfile.Flush();
                                        return;
                                    }

                                    values = Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString());
                                    WriteToFile(stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                                    //logfile.Flush();

                                    if ((values > val) && values != 0)
                                    {
                                        WriteToFile("Alarm Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                                        //logfile.Flush();

                                        THRES_AlarmCond[ID] = true;
                                        alarm = true;
                                        db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");

                                        

                                    }
                                    else
                                    {
                                        tosentmail += 1;
                                        WriteToFile("Alarm Condition Not Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                                        //logfile.Flush();

                                    }
                                    
                                    if (deadband.Trim() != "")
                                    {

                                        //Val = 13.0
                                        //deadband = 0.2
                                        //deadband_d = 13.0 - 0.2 = 12.8
                                        Double deadband_d = val - Convert.ToDouble(deadband);

                                        if (values < deadband_d)
                                        {
                                            WriteToFile("Deadband Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                            //logfile.Flush();
                                            if (Convert.ToBoolean(DeadbandMet[ID]) == false)
                                                DeadbandMet[ID] = true;

                                            WriteToFile("Setting alarm flag to false  for stations " + stnName + " sensor name " + sen_id);
                                            //logfile.Flush();
                                            condition[sen_id + "" + stnName] = true;
                                        }
                                        else
                                        {
                                            if (Convert.ToBoolean(DeadbandMet[ID]) == true)
                                            {
                                                WriteToFile("Deadband Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                                //logfile.Flush();
                                            }
                                            else
                                            {
                                                WriteToFile("Deadband Condition Not Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                                //logfile.Flush();

                                            }
                                        }
                                    }
                                    else
                                    {
                                        condition[sen_id + "" + stnName] = true;

                                    }
                                }
                                catch (Exception ex)
                                {
                                    condition[sen_id + "" + stnName] = true;

                                }
                                try
                                {
                                    if (isLastAlarmOnThisStationAndSensor(ID, logfile, "Threshold") == true)
                                        db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds.Tables[0].Rows[0][3] + "'", "", "", "");
                                }
                                catch (Exception e) { }
                            }
                            if (sign.Trim() == "<")
                            {
                                DataSet ds = null;
                                string sql_query = "";
                                try
                                {
                                    // logfile.WriteLine("<");
                                    logfile.Flush();

                                    string sensortype = getSensorType(stnfullname, sensorname);

                                    if (sensortype == "Real")
                                    {
                                        sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    }
                                    else
                                    {
                                        sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"VirtualValue\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    }
                                    //String sql_query = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\",b.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\"  and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' order by \"HydroMetShefCode\",t asc  limit 1";

                                    ds = db.getResultset(sql_query, "", "", "");
                                    // logfile.WriteLine("No of rows =>" + ds.Tables[0].Rows.Count);
                                    //logfile.Flush();

                                    DateTime date = Convert.ToDateTime(ds.Tables[0].Rows[0][0].ToString());
                                    Double mints = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Time"].ToString());
                                    date = date.AddMinutes(-mints);
                                    FD = date.Day.ToString();
                                    FY = date.Year.ToString();
                                    FM = date.Month.ToString();
                                    h = date.Hour.ToString();
                                    m = date.Minute.ToString();
                                    org_FD = FD;
                                    org_FY = FY;
                                    org_FM = FM;
                                    org_h = h;
                                    org_m = m;
                                    if (convert2Double(ds.Tables[0].Rows[0][2].ToString()) == -1)
                                    {
                                        // logfile.WriteLine("Null value in alarm for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                        //logfile.Flush();
                                        return;
                                    }

                                    values = Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString());
                                    WriteToFile(stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                                    logfile.Flush();

                                    if (values < val)
                                    {
                                        WriteToFile("Alarm Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                                        logfile.Flush();

                                        THRES_AlarmCond[ID] = true;
                                        alarm = true;
                                        db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");

                                    }
                                    else
                                    {
                                        tosentmail += 1;
                                        WriteToFile("Alarm Condition Not Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID);
                                        logfile.Flush();

                                    }
                                    
                                    if (deadband.Trim() != "")
                                    {


                                        Double deadband_d = val + Convert.ToDouble(deadband);

                                        if (values > deadband_d)
                                        {
                                            WriteToFile("Deadband Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                            //logfile.Flush();
                                            DeadbandMet[ID] = true;
                                            WriteToFile("Setting alarm flag to false  for stations " + stnName + " sensor name " + sen_id);
                                            //logfile.Flush();
                                            condition[sen_id + "" + stnName] = true;
                                        }
                                        else
                                        {
                                            if (Convert.ToBoolean(DeadbandMet[ID]) == true)
                                            {
                                                WriteToFile("Deadband Condition Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                                //logfile.Flush();
                                            }
                                            else
                                            {
                                                WriteToFile("Deadband Condition Not Met for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m + " Val " + values + " Alarm Id" + ID + " Deadband " + deadband_d);
                                                //logfile.Flush();

                                            }
                                        }
                                    }
                                    else
                                    {
                                        condition[sen_id + "" + stnName] = true;

                                    }
                                }
                                catch (Exception ex)
                                {
                                    condition[sen_id + "" + stnName] = true;

                                }
                                try
                                {
                                    if (isLastAlarmOnThisStationAndSensor(ID, logfile, "Threshold") == true)
                                        db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds.Tables[0].Rows[0][3] + "'", "", "", "");
                                }
                                catch (Exception e) { }
                            }
                            if (sign.Trim() == "<=")
                            {
                                DataSet ds = null;
                                try
                                {
                                    // logfile.WriteLine("<=");
                                    logfile.Flush();

                                    string sensortype = getSensorType(stnfullname, sensorname);

                                    if (sensortype == "Real")
                                    { }
                                    else { }
                                    //String sql_query = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\",b.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\"  and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' order by \"HydroMetShefCode\",t asc  limit 1";
                                    String sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    ds = db.getResultset(sql_query, "", "", "");
                                    // logfile.WriteLine("No of rows =>" + ds.Tables[0].Rows.Count);
                                    //logfile.Flush();

                                    DateTime date = Convert.ToDateTime(ds.Tables[0].Rows[0][0].ToString());
                                    Double mints = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Time"].ToString());
                                    date = date.AddMinutes(-mints);
                                    FD = date.Day.ToString();
                                    FY = date.Year.ToString();
                                    FM = date.Month.ToString();
                                    h = date.Hour.ToString();
                                    m = date.Minute.ToString();
                                    org_FD = FD;
                                    org_FY = FY;
                                    org_FM = FM;
                                    org_h = h;
                                    org_m = m;

                                    if (convert2Double(ds.Tables[0].Rows[0][2].ToString()) == -1)
                                    {
                                        WriteToFile("Null value in alarm for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                        //logfile.Flush();
                                        return;
                                    }

                                    values = Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString());

                                    if ((values <= val) && values != 0)
                                    {
                                        THRES_AlarmCond[ID] = true;
                                        alarm = true;
                                        db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");

                                    }
                                    else
                                    {
                                        tosentmail += 1;
                                    }
                                    
                                    if (deadband.Trim() != "")
                                    {


                                        Double deadband_d = val + Convert.ToDouble(deadband);

                                        if (values >= deadband_d)
                                        {
                                            WriteToFile("Values = " + values + " is less than deathband = " + deadband_d + " for stations " + stnName + " sensor name " + sen_id + " YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                            //logfile.Flush();
                                            DeadbandMet[ID] = true;
                                            WriteToFile("Setting alarm flag to false  for stations " + stnName + " sensor name " + sen_id);
                                            //logfile.Flush();
                                            condition[sen_id + "" + stnName] = true;
                                        }
                                    }
                                    else
                                    {
                                        condition[sen_id + "" + stnName] = true;

                                    }
                                }
                                catch (Exception ex)
                                {
                                    condition[sen_id + "" + stnName] = true;

                                }
                                try
                                {
                                    if (isLastAlarmOnThisStationAndSensor(ID, logfile, "Threshold") == true)
                                        db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds.Tables[0].Rows[0][3] + "'", "", "", "");
                                }
                                catch (Exception e) { }
                            }
                            if (sign.Trim() == ">=")
                            {
                                DataSet ds = null;
                                string sql_query = "";
                                try
                                {
                                    // logfile.WriteLine(">=");
                                    logfile.Flush();
                                    string sensortype = getSensorType(stnfullname, sensorname);

                                    if (sensortype == "Real")
                                    {
                                        sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    }
                                    else
                                    {
                                        sql_query = "select('20' ||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"VirtualValue\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    }
                                    //  String sql_query = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\",b.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\"  and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' order by \"HydroMetShefCode\",t asc  limit 1";

                                    ds = db.getResultset(sql_query, "", "", "");
                                    // logfile.WriteLine("No of rows =>" + ds.Tables[0].Rows.Count);
                                    //logfile.Flush();

                                    DateTime date = Convert.ToDateTime(ds.Tables[0].Rows[0][0].ToString());
                                    Double mints = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Time"].ToString());
                                    date = date.AddMinutes(-mints);
                                    FD = date.Day.ToString();
                                    FY = date.Year.ToString();
                                    FM = date.Month.ToString();
                                    h = date.Hour.ToString();
                                    m = date.Minute.ToString();
                                    org_FD = FD;
                                    org_FY = FY;
                                    org_FM = FM;
                                    org_h = h;
                                    org_m = m;

                                    if (convert2Double(ds.Tables[0].Rows[0][2].ToString()) == -1)
                                    {
                                        WriteToFile("Null value in alarm for " + stnName + " sensor name " + sen_id + "returning back YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                        //logfile.Flush();
                                        return;
                                    }

                                    values = Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString());

                                    if ((values >= val) && values != 0)
                                    {
                                        THRES_AlarmCond[ID] = true;
                                        alarm = true;
                                        db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");

                                    }
                                    else
                                    {
                                        tosentmail += 1;
                                    }
                                    if (deadband.Trim() != "")
                                    {


                                        Double deadband_d = val - Convert.ToDouble(deadband);

                                        if (values <= deadband_d)
                                        {
                                            WriteToFile("Values = " + values + " is less than deathband = " + deadband_d + " for stations " + stnName + " sensor name " + sen_id + " YEAR = " + FY + " MONTH " + FM + " DAY " + FD + " HOUR " + h + " MIN " + m);
                                            //logfile.Flush();
                                            DeadbandMet[ID] = true;
                                            WriteToFile("Setting alarm flag to false  for stations " + stnName + " sensor name " + sen_id);
                                            //logfile.Flush();
                                            condition[sen_id + "" + stnName] = true;
                                        }
                                    }
                                    else
                                    {
                                        condition[sen_id + "" + stnName] = true;

                                    }
                                }
                                catch (Exception ex)
                                {
                                    condition[sen_id + "" + stnName] = true;

                                }
                                try
                                {
                                    if (isLastAlarmOnThisStationAndSensor(ID, logfile, "Threshold") == true)
                                        db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds.Tables[0].Rows[0][3] + "'", "", "", "");
                                }
                                catch (Exception e) { }
                            }
                        }
                        else if (type == "Rate_of_Change")
                        {

                            // logfile.WriteLine("Rate_of_Change");
                            logfile.Flush();

                            if (sign.Trim() == "+")
                            {
                                DataSet ds2 = null;
                                string sql_query2 = "";
                                try
                                {
                                    // logfile.WriteLine("+");
                                    logfile.Flush();
                                    // String sql_query2 = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t,  \"HydroMetShefCode\", \"VirtualValue\" as \"Value\",b.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' order by \"HydroMetShefCode\",t limit 1";
                                    //  String pv = "SELECT \"VirtualValue\" as \"Value\" FROM public.\"tblAlarm2Sensor\" where created_at<now() order by \"created_at\" desc  limit 1";
                                    string sensortype = getSensorType(stnfullname, sensorname);

                                    if (sensortype == "Real")
                                    {
                                        sql_query2 = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    }
                                    else
                                    {
                                        sql_query2 = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"VirtualValue\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    }

                                    ds2 = db.getResultset(sql_query2, "", "", "");
                                    // logfile.WriteLine("No of rows =>" + ds2.Tables[0].Rows.Count);
                                    logfile.Flush();

                                    DateTime date = Convert.ToDateTime(ds2.Tables[0].Rows[0][0].ToString());
                                    int HydroMetParamsTypeId = Convert.ToInt32(sen_id);
                                    //string  HydroMetShefCode = (dsAlarm.Tables[0].Rows[0]["HydroMetShefCode"].ToString());
                                    FD = date.Day.ToString();
                                    FY = date.Year.ToString().Substring(2, 2);
                                    FM = date.Month.ToString();
                                    h = date.Hour.ToString();
                                    m = date.Minute.ToString();
                                    sec = date.Second.ToString();

                                    org_FD = FD;
                                    org_FY = FY;
                                    org_FM = FM;
                                    org_h = h;
                                    org_m = m;


                                    int hh = date.Hour;
                                    int MM = date.Minute;
                                    int ss = date.Second;
                                    int SpanTime = Convert.ToInt32(dsAlarm.Tables[0].Rows[0]["Time"]);

                                    DateTime Pdate = date.AddMinutes(-SpanTime);

                                    //TimeSpan ts = new TimeSpan(hh, MM, ss);
                                    //TimeSpan PDate = ts.Subtract(new TimeSpan(0, SpanTime, 0));


                                    int hr = Pdate.Hour;
                                    int mn = Pdate.Minute;
                                    int sc = Pdate.Second;
                                    FD = Pdate.Day.ToString();
                                    FY = Pdate.Year.ToString().Substring(2, 2);
                                    FM = Pdate.Month.ToString();
                                    //  string Pvalue = "SELECt Value FROM 'tblStation_SF14_18' where Year=18 and Month=1 and Day=1 and Hour=0 and Minute=30 and Second=0 AND AlarmFlag=0 ANd HydroMetParamsTypeId=167 ";

                                    string Pvalue = "SELECt \"Value\" as \"Value\",\"AlarmFlag\" from public.\"" + table + "\"  where \"Year\"=" + FY + " and \"Month\"=" + FM + " and \"Day\"=" + FD + " and \"Hour\"=" + hr + " and \"Minute\"=" + mn + " and \"Second\"=" + sec + " ANd \"HydroMetParamsTypeId\"=" + sen_id + " ";
                                    //string Pvalue = "SELECt \"VirtualValue\" as \"Value\" FROM public.\"'tblStation_SF14_18'\" where \"Year\"=18 and \"Month\"=1 and \"Day\"=1 and \"Hour\"=0 and \"Minute\"=30 and \"Second\"=0 AND \"AlarmFlag\"='0' ANd \"HydroMetParamsTypeId\"=167 ";
                                    //  string Pvalue = "SELECT \"VirtualValue\" as \"Value\" FROM public.\"'tblStation_SF10_18'\" where \"Year\"=18 and \"Month\"=1 and \"Day\"=1 and \"Hour\"=1 and \"Minute\"=0 and \"Second\"=0";
                                    //string Pvalue = "SELECt \"VirtualValue\" as \"Value\" from \"" + table + "\" a where \"Year\"=" + FY + " and \"Month\"=" + FM+ " and \"Day\"=" + FD + " and \"Hour\"=" + hr + " and \"Minute\"=" + mn + " and \"Second\"=" + sc + " AND \"AlarmFlag\"='0' ANd \"HydroMetParamsTypeId\"=" + sen_id + "";
                                    // string Pvalue = "SELECt \"VirtualValue\" as \"Value\" FROM public.\'" + table + "\'";
                                    // string Pvalue = "SELECT \"VirtualValue\" as \"Value\" FROM public.\"'"+table+"'\" where \"Year\"=18 and \"Month\"=1 and \"Day\"=1 and \"Hour\"=6 and \"Minute\"=45 and \"Second\"=0";

                                    //  Double PDate  = Convert.ToDouble(date.AddMinutes(-mints));
                                    // string Pvalue = "Select \"VirtualValue\" as \"Value\"   from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' and \"Hour\"=" + hr + " and \"Minute\"=" + mn + " and \"Second\"=" + sec + " and \"Year\"=" + FY + " and \"Month\"=" + FM + " and \"Day\"=" + FD + "";
                                    //   string Pvalue = "Select \"VirtualValue\" as \"Value\"   from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  and \"AlarmFlag\"!='11' and \"Hour\"='" + hr + "' and \"Minute\"='" + mn + "' and \"Second\"='" + sec + "' and \"Year\"='"+FY+"' and \"Month\"='"+FM+"' and \"Day\"='"+FD+"'";
                                    // string Pvalue = "Select Value  from \"" + table + "\" a, where Change=" + sign+" and AlarmName="+alarmName+" and Type="+type+" and Hour="+hr+" and Minute="+mn+" and Second="+sec+"";
                                    // string Sp = "select   \"Time\", from \"tblAlarm2Sensor\" where \"created_at\"="+date+"";
                                    // string Sp = "SELECT  \"Time\" FROM public.\""+table+"\" where \"VirtualValue\" as \"Value\"=40";
                                    // int i = Convert.ToInt32(cmd.ExecuteScalar());
                                    DataSet dPrevValue = db.getResultset(Pvalue, "", "", "");

                                    if (dPrevValue != null && dPrevValue.Tables[0].Rows.Count > 0)
                                    {
                                        Double PreviousValue = Convert.ToDouble(dPrevValue.Tables[0].Rows[0][0]);
                                        Double CurrentValue = Convert.ToDouble(ds2.Tables[0].Rows[0][2].ToString().Trim());
                                        values = CurrentValue;
                                        double AP = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Value"]);

                                        // logfile.WriteLine("PreviousValue = " + PreviousValue + "; CurrentValue = " + CurrentValue + "; AP = " + AP);
                                        logfile.Flush();

                                        //Double PreviousValue = 15.3;
                                        //Double AP = 0.1;
                                        //Double CurrentValue = 16.3;
                                        Double ROCT = PreviousValue + AP;
                                        if (CurrentValue >= ROCT)
                                        {
                                            alarm = true;
                                            CD_RateOfChange = date;

                                            // db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds2.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds2.Tables[0].Rows[0][3] + "'", "", "", "");
                                            db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");
                                        }
                                        else
                                        {
                                            tosentmail += 1;
                                            alarm = false;
                                        }
                                        // string PreviousValue = "Select Value from "+table+" where Minutes="+PDate+" ";
                                        ////m = date.Minute.ToString();
                                        // String str1 = ds2.Tables[0].Rows[0][2].ToString().Trim();


                                        //Double v1 = val + Convert.ToDouble(ds.Tables[0].Rows[0][2].ToString().Trim());
                                    }


                                }
                                catch (Exception exc)
                                { }
                                try
                                {
                                    if (isLastAlarmOnThisStationAndSensor(ID, logfile, "ROC") == true)
                                    {
                                        DateTime date = Convert.ToDateTime(ds2.Tables[0].Rows[0][0].ToString());
                                        FD = date.Day.ToString();
                                        FY = date.Year.ToString().Substring(2, 2);
                                        FM = date.Month.ToString();
                                        h = date.Hour.ToString();
                                        m = date.Minute.ToString();
                                        sec = date.Second.ToString();

                                        string AlarmTrue = "Update public.\"" + table + "\" set \"AlarmFlag\"='11' Where \"Year\"=" + FY + " and \"Month\"=" + FM + " and \"Day\"=" + FD + " and \"Hour\"=" + h + " and \"Minute\"=" + m + " and \"Second\"=" + sec + " ANd \"HydroMetParamsTypeId\"=" + sen_id + " ";
                                        db.ExecuteQuery(AlarmTrue, "", "", "");
                                    }
                                }
                                catch (Exception e) { }
                                Double v2 = val + Convert.ToDouble(ds2.Tables[0].Rows[0][2].ToString().Trim());
                            }
                            if (sign.Trim() == "-")
                            {
                                DataSet ds1 = null;
                                string sql_query1 = "";
                                try
                                {
                                    // logfile.WriteLine("-");
                                    logfile.Flush();
                                    string sensortype = getSensorType(stnfullname, sensorname);

                                    if (sensortype == "Real")
                                    {
                                        sql_query1 = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"Value\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    }
                                    else
                                    {
                                        sql_query1 = "select ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp as t, b.\"HydroMetShefCode\", \"VirtualValue\" as \"Value\",a.\"HydroMetParamsTypeId\" from \"" + table + "\" a, \"tblHydroMetParamsType\" b where b.\"HydroMetParamsTypeId\"= a.\"HydroMetParamsTypeId\" and a.\"HydroMetParamsTypeId\"='" + sen_id + "'  order by 1 desc,t limit 1";
                                    }

                                    //String pv = "SELECT \"VirtualValue\" as \"Value\" FROM public.\"tblAlarm2Sensor\" where created_at<now() order by \"created_at\" desc  limit 1";

                                    ds1 = db.getResultset(sql_query1, "", "", "");
                                    // logfile.WriteLine("No of rows =>" + ds1.Tables[0].Rows.Count);

                                    logfile.Flush(); String strDate = ds1.Tables[0].Rows[0][0].ToString();
                                    DateTime date = Convert.ToDateTime(strDate);

                                    FD = date.Day.ToString();
                                    FY = date.Year.ToString().Substring(2, 2);
                                    FM = date.Month.ToString();
                                    h = date.Hour.ToString();
                                    m = date.Minute.ToString();

                                    org_FD = FD;
                                    org_FY = FY;
                                    org_FM = FM;
                                    org_h = h;
                                    org_m = m;


                                    h = date.Hour.ToString();
                                    m = date.Minute.ToString();
                                    sec = date.Second.ToString();


                                    int hh = date.Hour;
                                    int MM = date.Minute;
                                    int ss = date.Second;
                                    int SpanTime = Convert.ToInt32(dsAlarm.Tables[0].Rows[0]["Time"]);


                                    DateTime Pdate = date.AddMinutes(-SpanTime);

                                    //TimeSpan ts = new TimeSpan(hh, MM, ss);
                                    //TimeSpan PDate = ts.Subtract(new TimeSpan(0, SpanTime, 0));


                                    int hr = Pdate.Hour;
                                    int mn = Pdate.Minute;
                                    int sc = Pdate.Second;
                                    FD = Pdate.Day.ToString();
                                    FY = Pdate.Year.ToString().Substring(2, 2);
                                    FM = Pdate.Month.ToString();


                                    string Pvalue1 = "SELECt \"Value\" as \"Value\",\"AlarmFlag\" from public.\"" + table + "\"  where \"Year\"=" + FY + " and \"Month\"=" + FM + " and \"Day\"=" + FD + " and \"Hour\"=" + hr + " and \"Minute\"=" + mn + " and \"Second\"=" + sec + " ANd \"HydroMetParamsTypeId\"=" + sen_id + " ";
                                    DataSet dPrevValue = db.getResultset(Pvalue1, "", "", "");
                                    if (dPrevValue != null && dPrevValue.Tables[0].Rows.Count > 0)
                                    {
                                        Double PreviousValue1 = Convert.ToDouble(dPrevValue.Tables[0].Rows[0][0]);
                                        Double CurrentValue1 = Convert.ToDouble(ds1.Tables[0].Rows[0][2].ToString().Trim());
                                        values = CurrentValue1;
                                        double AP = Convert.ToDouble(dsAlarm.Tables[0].Rows[0]["Value"]);
                                        //Double PreviousValue = 11.3;
                                        //Double AP = 0.5;
                                        //Double CurrentValue = 9.3;
                                        Double ROCT = PreviousValue1 - AP;
                                        if (CurrentValue1 <= ROCT)
                                        {
                                            alarm = true;
                                            CD_RateOfChange = date;

                                            // db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='11' where ('20'||\"Year\"::text ||'-'|| \"Month\"::text ||'-'|| \"Day\" ::text ||' '|| \"Hour\"::text || ':' || \"Minute\" ::text || ':' ||\"Second\")::timestamp='" + ds2.Tables[0].Rows[0][0] + "' AND \"HydroMetParamsTypeId\"='" + ds2.Tables[0].Rows[0][3] + "'", "", "", "");
                                            db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='true' where \"StationShefCode\"='" + stnName + "'", "", "", "");
                                        }
                                        else
                                        {
                                            tosentmail += 1;
                                            alarm = false;
                                        }
                                    }

                                }
                                catch (Exception exc)
                                { }
                                try
                                {
                                    if (isLastAlarmOnThisStationAndSensor(ID, logfile, "ROC") == true)
                                    {
                                        String strDate = ds1.Tables[0].Rows[0][0].ToString();
                                        DateTime date = Convert.ToDateTime(strDate);

                                        FD = date.Day.ToString();
                                        FY = date.Year.ToString().Substring(2, 2);
                                        FM = date.Month.ToString();
                                        h = date.Hour.ToString();
                                        m = date.Minute.ToString();
                                        org_FD = FD;
                                        org_FY = FY;
                                        org_FM = FM;
                                        org_h = h;
                                        org_m = m;

                                        string AlarmTrue = "Update public.\"" + table + "\" set \"AlarmFlag\"='11' Where \"Year\"=" + FY + " and \"Month\"=" + FM + " and \"Day\"=" + FD + " and \"Hour\"=" + h + " and \"Minute\"=" + m + " and \"Second\"=" + sec + " ANd \"HydroMetParamsTypeId\"=" + sen_id + " ";
                                        db.ExecuteQuery(AlarmTrue, "", "", "");
                                    }
                                }
                                catch (Exception e) { }
                            }

                        }
                        else
                        {
                            WriteToFile(type + " Does not exist");
                            logfile.Flush();
                        }

                        if (alarm)
                        {

                            DataSet dsAlarmUser = db.getResultset("select \"communicationway\",\"Username\" from \"tblAlarmType2User\" where \"AlarmName\"='" + alarmName + "'", "", "", "");
                            if (dsAlarmUser == null)
                                return;


                            for (int i = 0; i < dsAlarmUser.Tables[0].Rows.Count; i++)
                            {

                                string usersvalues = dsAlarmUser.Tables[0].Rows[i]["Username"].ToString();
                                string substr1 = "Group - ";
                                if (usersvalues.Contains(substr1))
                                {
                                    DataSet dsAlarmUsernew = db.getResultset("select \"GroupCommunicationWay\" from \"tblloginandregister\"", "", "", "");
                                    if (dsAlarmUsernew.Tables[0].Rows[i]["GroupCommunicationWay"].ToString().Trim() != "" || dsAlarmUsernew.Tables[0].Rows[i]["GroupCommunicationWay"].ToString().Trim() != null)
                                    {
                                        string checkusergroup = usersvalues.Replace("Group - ", "").ToString();
                                        DataSet dsemailed = db.getResultset("select \"Email\",\"mobileno\",\"GroupName\",\"GroupCommunicationWay\" from \"tblloginandregister\"", "", "", "");
                                        for (int k = 0; k < dsemailed.Tables[0].Rows.Count; k++)
                                        {
                                            string temp = dsemailed.Tables[0].Rows[k]["GroupName"].ToString().Trim();
                                            string tempway = dsemailed.Tables[0].Rows[k]["GroupCommunicationWay"].ToString().Trim();
                                            char[] spearator = { ',' };
                                            string[] groupusers = temp.Split(spearator);
                                            string[] groupusersway = tempway.Split(spearator);

                                            for (int q = 0; q < groupusers.Length; q++)
                                            //foreach (string usergroups in groupusers)
                                            {
                                                if (groupusers[q] == checkusergroup)
                                                {
                                                    if (groupusersway[q].ToString().Trim() == "Email" || groupusersway[q].ToString().Trim() == "Both")
                                                    {
                                                        if (dsemailed.Tables[0].Rows[k]["Email"].ToString().Trim() != null)
                                                        {
                                                            mail.To.Add(dsemailed.Tables[0].Rows[k]["Email"].ToString().Trim());
                                                            WriteToFile("Alarm ID =" + ID + " EMail Added to Alarm: " + dsemailed.Tables[0].Rows[k]["Email"].ToString().Trim());
                                                            mail.To.Add(dsemailed.Tables[0].Rows[k]["mobileno"].ToString().Trim() + "@tmomail.net");
                                                            WriteToFile("Alarm ID =" + ID + " Mobile Added to Alarm: " + dsemailed.Tables[0].Rows[k]["mobileno"].ToString().Trim()
                                                               );
                                                            lstMailIds.Add(dsemailed.Tables[0].Rows[k]["Email"].ToString().Trim());

                                                        }
                                                        else
                                                        {
                                                            WriteToFile("Alarm ID =" + ID + " No email id and number is available");
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                else

                                {
                                    if (dsAlarmUser.Tables[0].Rows[i]["communicationway"].ToString().Trim() == "Email" || dsAlarmUser.Tables[0].Rows[i]["communicationway"].ToString().Trim() == "Both")
                                    {
                                        DataSet dsemail = db.getResultset("select \"Email\",\"mobileno\" from \"tblloginandregister\" where \"Username\"='" + usersvalues + "'", "", "", "");
                                        if (dsemail != null)
                                        {
                                            mail.To.Add(dsemail.Tables[0].Rows[0]["Email"].ToString().Trim());
                                            WriteToFile("Alarm ID =" + ID + " EMail Added to Alarm: " + dsemail.Tables[0].Rows[0]["Email"].ToString().Trim());
                                            mail.To.Add(dsemail.Tables[0].Rows[0]["mobileno"].ToString().Trim() + "@tmomail.net");
                                            WriteToFile("Alarm ID =" + ID + " Mobile Added to Alarm: " + dsemail.Tables[0].Rows[0]["mobileno"].ToString().Trim());
                                        }
                                        else
                                        {
                                            WriteToFile("Alarm ID =" + ID + " No email id and number is available");
                                        }
                                    }
                                }
                            }

                            if (type == "Threshold_Value")//if type =Threshold value and change = ">"
                            {

                                if (Convert.ToBoolean(THRES_AlarmCond[ID]) == true)
                                {

                                    // First time case
                                    if (Convert.ToBoolean(THRES_Email[ID]) == false)//false
                                    {
                                        WriteToFile("Sending email for first time and not checking deathband with body " + body + " for stations " + stnName + " sensor name " + sen_id + " value " + values);
                                        WriteToFile("Sending email with body " + body + " for stations " + stnName + " sensor name " + sen_id + " value " + values);
                                        logfile.Flush();
                                        //String stnFullName = db.getstationname(stnName);
                                        //String senFullName = db.getsensorname(stnfullname, stnName, sensor);
                                        body += ("Station Name: " + stnfullname + "\n" + "Sensor Name: " + sensorname + "\n" + "Current Value:" + values + "\n" + "Time Stamp: " + org_FY + "-" + Convert.ToInt32(org_FM).ToString("00") + "-" + Convert.ToInt32(org_FD).ToString("00") + " " + Convert.ToInt32(org_h).ToString("00") + ":" + Convert.ToInt32(org_m).ToString("00"));
                                        combinedBody += "\n\n" + body;

                                    }
                                    else
                                    {
                                        // not first time checking dead band
                                        if (Convert.ToBoolean(DeadbandMet[ID]) == true)//true
                                        {

                                            WriteToFile("Sending email with body " + body + " for stations " + stnName + " sensor name " + sen_id + " value " + values);
                                            logfile.Flush();
                                            WriteToFile("2ndR");
                                            String stnFullName = db.getstationname(stnName);//gets station fullname
                                            String senFullName = db.getsensorname(stnFullName, stnName, sensor);//gets station sensorname 
                                            body += ("Station Name: " + stnFullName + "\n" + "Sensor Name: " + senFullName + "\n" + "Current Value:" + values + "\n" + "Time Stamp: " + org_FY + "-" + Convert.ToInt32(org_FM).ToString("00") + "-" + Convert.ToInt32(org_FD).ToString("00") + " " + Convert.ToInt32(org_h).ToString("00") + ":" + Convert.ToInt32(org_m).ToString("00"));
                                            combinedBody += "\n" + body;
                                            //creates body of the mail 
                                            // email(mail, body);// call email() function tosend email to user 
                                            //SendMail(lstMailIds, body);
                                            // THRES_Email[ID] = true;
                                            // DeadbandMet[ID] = false;
                                            //THRES_AlarmCond[ID] = false;

                                            WriteToFile("Setting alarm flag to true  for stations " + stnName + " sensor name " + sen_id);
                                            logfile.Flush();

                                        }
                                        else
                                        {
                                            WriteToFile("Ignoring alarm  for stations " + stnName + " sensor name " + sen_id + " value " + values);
                                            logfile.Flush();

                                        }

                                    }
                                }


                            }
                            else
                            {
                                DateTime? dt_alarmSent = null;

                                if (ROC_AlarmSent[ID] != null)
                                {
                                    dt_alarmSent = (DateTime)ROC_AlarmSent[ID];
                                }
                                if (ROC_AlarmSent[ID] == null)
                                {
                                    WriteToFile("Sending email with body " + body + " for stations " + stnName + " sensor name " + sen_id + " value " + values);
                                    logfile.Flush();
                                    WriteToFile("3rdR");
                                    String stnFullName = db.getstationname(stnName);//gets station fullname 
                                    String senFullName = db.getsensorname(stnFullName, stnName, sensor);//gets station sensorname 
                                    body += ("Station Name: " + stnFullName + "\n" + "Sensor Name: " + senFullName + "\n" + "Current Value:" + values + "\n" + "Time Stamp: " + org_FY + ":" + Convert.ToInt32(org_FM).ToString("00") + ":" + Convert.ToInt32(org_FD).ToString("00") + " " + Convert.ToInt32(org_h).ToString("00") + ":" + Convert.ToInt32(org_m).ToString("00"));
                                    //creates body of the mail 
                                    combinedBody += "\n\n" + body;
                                    //email(mail, body);// call email() function tosend email to user  
                                                      //SendMail(lstMailIds, body);
                                    ROC_AlarmSent[ID] = CD_RateOfChange;


                                    WriteToFile("Setting alarm flag to true  for stations " + stnName + " sensor name " + sen_id);
                                    logfile.Flush();
                                }
                                else if (CD_RateOfChange.Value.Subtract(dt_alarmSent.Value) >= TimeSpan.FromMinutes(mints))
                                {
                                    TimeSpan bal_time = CD_RateOfChange.Value.Subtract(dt_alarmSent.Value);
                                    WriteToFile("Sending email with body " + body + " for stations " + stnName + " sensor name " + sen_id + " value " + values + " and balance time span is " + bal_time);
                                    logfile.Flush();
                                    String stnFullName = db.getstationname(stnName);//gets station fullname 
                                    String senFullName = db.getsensorname(stnFullName, stnName, sensor);//gets station sensorname 
                                    body += ("Station Name: " + stnFullName + "\n" + "Sensor Name: " + senFullName + "\n" + "Current Value:" + values + "\n" + "Time Stamp: " + org_FY + "-" + Convert.ToInt32(org_FM).ToString("00") + "-" + Convert.ToInt32(org_FD).ToString("00") + " " + Convert.ToInt32(org_h).ToString("00") + ":" + Convert.ToInt32(org_m).ToString("00"));
                                    combinedBody += "\n\n" + body;
                                    //creates body of the mail 
                                    //email(mail, body);// call email() function tosend email to user 
                                                      // SendMail(lstMailIds, body);
                                    ROC_AlarmSent[ID] = CD_RateOfChange;


                                    WriteToFile("Setting alarm flag to true  for stations " + stnName + " sensor name " + sen_id);
                                    logfile.Flush();
                                }
                                else
                                {


                                    TimeSpan bal_time = CD_RateOfChange.Value.Subtract(dt_alarmSent.Value);
                                    WriteToFile("Ignoring alarm  for stations " + stnName + " sensor name " + sen_id + " value " + values + " because timespan is " + bal_time.Minutes + " at " + CD_RateOfChange.Value);
                                    logfile.Flush();

                                }
                            }
                            if (deadband.Trim() != "")
                                condition[sen_id + "" + stnName] = false;
                            
                        }
                        else
                        {
                            //update alarmflag to false where StationShefCode =stnName
                            db.ExecuteQuery("update \"tblStationLocation\" set \"AlarmFlag\"='false' where \"StationShefCode\"='" + stnName + "'", "", "", "");

                        }
                        //db.ExecuteQuery("update \"" + table + "\" set \"AlarmFlag\"='1' where \"AlarmFlag\"!='11'", "", "", "");
                        //set alarm to false 
                        alarm = false;

                    }
                    catch (Exception ex)
                    {
                        WriteToFile("Exception " + ex.Message);
                        logfile.Flush();
                    }
                }
            }
            if (tosentmail == 0)
            {
                email(mail, combinedBody);
                //SendMail(lstMailIds, body);
                THRES_Email[ID] = true; // Alarm is sent so flagging as true so that email is not sent second time
                THRES_AlarmCond[ID] = false;
                DeadbandMet[ID] = false;
                WriteToFile("Setting alarm flag to true  for stations " + stnName + " sensor name " + sen_id);
                logfile.Flush();
            }
           

        }

        private string getSensorType(string stnfullname, string sensorname)
        {
            DataSet ds = db.getResultset("select \"SensorType\" from \"SensorValues\" where \"StationFullName\"='" + stnfullname + "' and \"Sensor\"='" + sensorname + "'", "", "", "");
            string typevalue = ds.Tables[0].Rows[0]["SensorType"].ToString();
            return typevalue;
        }

        /* uSed for sending mail to user emailID regarding this sensor values 
         input parameters : mail-> MailId, boby-> Mail body */
        public void email(System.Net.Mail.MailMessage mail, String body)
        {
            try
            {
                //writing the mail to logofile to check the process and data which we are sending thorugh email


                //WriteToFile(mail.To.ToString());
                //logfile.Flush();

                //WriteToFile(body);
                //logfile.Flush();
                // to = "zeeshanrazak@gmail.com";
                //string from = ConfigurationManager.ConnectionStrings["email"].ConnectionString;
                //string sm= ConfigurationManager.ConnectionStrings["smtp"].ConnectionString;
                //int po=Convert.ToInt32(ConfigurationManager.ConnectionStrings["port"].ConnectionString);
                //string pass= ConfigurationManager.ConnectionStrings["pass"].ConnectionString;
                String alarm = ConfigurationManager.ConnectionStrings["Alarm"].ConnectionString;
                WriteToFile(alarm + " " + DateTime.Now);
                logfile.Flush();

                if (alarm.Trim().Length == 0)
                    return;
                String[] alarm_setting = alarm.Split(';');
                //for (int i = 0; i < alarm_setting.Length; i++)
                //{
                //    logfile.WriteLine(alarm_setting[i]);
                //    logfile.Flush();
                //}


                //string from = "kyocera@southfeather.com";
                string from = alarm_setting[2].Trim();
                //create the mail message

                //set the FROM address
                mail.From = new MailAddress(from);
                //set the RECIPIENTS
                //mail.To.Add(to);
                //enter a SUBJECT
                mail.Subject = "Warning";
                //Enter the message BODY
                mail.Body = body;
                //"Alarm Email for Station " + stnName + " Current Value of this Station at sensor " + sensor + " is :" + values;

                //set the mail server (default should be smtp.1and1.com)
                //SmtpClient smtp = new SmtpClient("smtp.southfeather.com");
                SmtpClient smtp = new SmtpClient(alarm_setting[0].Trim());
                //SmtpClient smtp = new SmtpClient(sm);
                //Enter your full e-mail address and password
                //smtp.Port = 25;
                smtp.Port = Convert.ToInt32(alarm_setting[1].Trim());
                // smtp.Port = po;
                smtp.UseDefaultCredentials = false;
                smtp.EnableSsl = true;

                smtp.Credentials = new NetworkCredential(from, alarm_setting[3].Trim());
                //smtp.Credentials = new NetworkCredential(from, pass);


                //send the message 
                WriteToFile("Before Sending email");
                logfile.Flush();
                smtp.Send(mail);


                WriteToFile("" + DateTime.Now + " - Email sent with body " + body);
                logfile.Flush();

            }
            catch (Exception ex)
            {
                WriteToFile(ex.Message + "" + DateTime.Now + " - " + ex.Message);
                logfile.Flush();
                WriteToFile(ex.StackTrace + "" + DateTime.Now + " - " + ex.Message);
                logfile.Flush();
            }
        }

        public void SendMail(List<string> ToMail, String body)
        {
            try
            {
                string emailSender = System.Configuration.ConfigurationManager.AppSettings["emailsender"].ToString();
                string emailSenderPassword = System.Configuration.ConfigurationManager.AppSettings["password"].ToString();
                string emailSenderHost = ConfigurationManager.AppSettings["smtpserver"].ToString();
                int emailSenderPort = Convert.ToInt16(ConfigurationManager.AppSettings["portnumber"]);
                Boolean emailIsSSL = Convert.ToBoolean(ConfigurationManager.AppSettings["IsSSL"]);
                //string FilePath = @"C:\Gyana\NewProject\NewProject\EmailTemplates\SignUp.html";
                //StreamReader str = new StreamReader(FilePath);
                //string MailText = str.ReadToEnd();
                //str.Close();
                //MailText = MailText.Replace("[Gyana]", txtUser.Text.Trim());
                string subject = "Warning";
                //string ToMail = ";";
                MailMessage _mailmsg = new MailMessage();
                _mailmsg.IsBodyHtml = true;
                _mailmsg.From = new MailAddress(emailSender);
                //_mailmsg.To.Add(ToMail);
                //string[] multi = { "", "" };
                //string[] multi = ToMail.Split(',');
                foreach (string Multiemail in ToMail)
                {
                    _mailmsg.To.Add(new MailAddress(Multiemail));
                }
                _mailmsg.Subject = subject;
                _mailmsg.Body = body;
                SmtpClient _smtp = new SmtpClient();
                _smtp.Host = emailSenderHost;
                _smtp.Port = emailSenderPort;
                _smtp.EnableSsl = emailIsSSL;
                NetworkCredential _network = new NetworkCredential(emailSender, emailSenderPassword);
                _smtp.Credentials = _network;
                _smtp.Send(_mailmsg);

            }
            catch (Exception ex)
            {

                throw;
            }
        }

        public void WriteToFile(string Message)
        {

            string path = AppDomain.CurrentDomain.BaseDirectory + "\\Logs";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "\\Logs\\ServiceLog_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
            if (!File.Exists(filepath))
            {
                // Create a file to write to.   
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
        }

        protected override void OnStop()
        {
            //on stop stop of service 
            Hashtable ROC_AlarmSent = new Hashtable();
            Hashtable THRES_AlarmCond = new Hashtable();
            Hashtable THRES_Email = new Hashtable();
            Hashtable condition = new Hashtable();
            Hashtable DeadbandMet = new Hashtable();

            bRunThread = false;

            // serviceStarted = false;
            // wait for threads to stop
            //faxWorkerThread.Join(60);
            // File.Create(AppDomain.CurrentDomain.BaseDirectory + "OnStop.txt");

        }
    }
}
