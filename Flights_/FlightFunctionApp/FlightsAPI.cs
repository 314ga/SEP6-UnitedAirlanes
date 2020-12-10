using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Data.SqlClient;
using System.Data;

namespace FlightFunctionApp
{
    public static class FlightsAPI
    {
        [FunctionName("FlightsAPI")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string userRequest = req.Query["requestBody"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            userRequest = userRequest ?? data?.requestBody;
            string responseMessage = "";
                      if (!string.IsNullOrEmpty(userRequest))
            { 
                //check if request is null or empty

                // Get the connection string from app settings and use it to create a connection.
                var str = Environment.GetEnvironmentVariable("sqldb_connection");
                using (SqlConnection conn = new SqlConnection(str))
                {
                    conn.Open();
                    var dbQuery = "";
                    switch (userRequest)
                    {
                        /*Total number of flights per month*/
                        case "flights-per-month":
                            dbQuery = "DECLARE @FLIGHT_TABLE  table(year int, flights int) INSERT INTO @FLIGHT_TABLE SELECT year, COUNT(*) " +
                                "FROM dbo.flights WHERE year = 2013 GROUP BY month, year ORDER BY month; " +
                                "Declare @flg as varchar(MAX) = (SELECT REPLACE(REPLACE((SELECT flights = STUFF((SELECT ', ' + cast(flights as nvarchar(50)) FROM  @FLIGHT_TABLE " +
                                "AS x2 WHERE year = x.year ORDER BY year FOR XML PATH('')), 1, 1, '') FROM @FLIGHT_TABLE AS x GROUP BY year ORDER BY year FOR JSON PATH)," +
                                "'[{\"flights\":\"','{\"flights\":['),'\"}]',']}')); SELECT @flg";
                            break;
                        case "flights-per-month-stacked":dbQuery = "DECLARE @FLIGHT_PM_TABLE  table(origin varchar(5), flights int) INSERT INTO @FLIGHT_PM_TABLE SELECT origin, COUNT(*) AS month_flights_origins " +
                                "FROM dbo.flights WHERE origin = 'JFK' AND year = 2013 GROUP BY origin, month, year " +
                                "Declare @fjfk as varchar(MAX) = (SELECT REPLACE(REPLACE((SELECT jfk = STUFF((SELECT ', ' + cast(flights as nvarchar(50)) FROM  @FLIGHT_PM_TABLE " +
                                "AS x2 WHERE origin = x.origin ORDER BY origin FOR XML PATH('')), 1, 1, '') FROM @FLIGHT_PM_TABLE AS x GROUP BY origin ORDER BY origin FOR JSON PATH),'[{\"jfk\":\"','{\"jfk\":['),'\"}]','],')); " +
                                "DECLARE @FLIGHT_PM_TABLEEWR  table(origin varchar(5), flights int) " +
                                "INSERT INTO @FLIGHT_PM_TABLEEWR SELECT origin, COUNT(*) AS month_flights_origins " +
                                "FROM dbo.flights WHERE origin = 'EWR' AND year = 2013 GROUP BY origin, month, year " +
                                "Declare @fewr as varchar(MAX) = (SELECT REPLACE(REPLACE((SELECT ewr = STUFF((SELECT ', ' + cast(flights as nvarchar(50)) FROM  @FLIGHT_PM_TABLEEWR " +
                                "AS x2 WHERE origin = x.origin ORDER BY origin FOR XML PATH('')), 1, 1, '') FROM @FLIGHT_PM_TABLEEWR AS x GROUP BY origin ORDER BY origin FOR JSON PATH),'[{\"ewr\":\"','\"ewr\":['),'\"}]','],')); " +
                                "DECLARE @FLIGHT_PM_TABLELGA  table(origin varchar(5), flights int) " +
                                "INSERT INTO @FLIGHT_PM_TABLELGA SELECT origin, COUNT(*) AS month_flights_origins FROM dbo.flights WHERE origin = 'LGA' AND year = 2013 GROUP BY origin, month, year " +
                                "Declare @flga as varchar(MAX) = (SELECT REPLACE(REPLACE((SELECT lga = STUFF((SELECT ', ' + cast(flights as nvarchar(50)) FROM  @FLIGHT_PM_TABLELGA " +
                                "AS x2 WHERE origin = x.origin ORDER BY origin FOR XML PATH('')), 1, 1, '') FROM @FLIGHT_PM_TABLELGA AS x GROUP BY origin ORDER BY origin FOR JSON PATH),'[{\"lga\":\"','\"lga\":['),'\"}]',']}')); " +
                                "Select CONCAT(@fjfk, @fewr, @flga); ";
                            break;
                        /*Top 10 destinations and number of flights*/
                        case "top-dest-table":

                            dbQuery = "SELECT TOP 10 dest, count(*) AS number_of_flights " +
                                "FROM dbo.flights GROUP BY dest ORDER BY number_of_flights DESC FOR JSON PATH; ";
                            break;
                        case "top-dest":
                            dbQuery = "DECLARE @LOCAL_TABLEVARIABLE  table(origin varchar(20),dest varchar(5),JFK int) INSERT INTO @LOCAL_TABLEVARIABLE " +
                                "SELECT origin,result_table_dest, COUNT(*) AS JFK FROM(SELECT TOP 10 dest AS result_table_dest, count(*) AS number_of_flights " +
                                "FROM dbo.flights GROUP BY dest ORDER BY number_of_flights DESC) AS result_table INNER JOIN dbo.flights " +
                                "ON result_table.result_table_dest = dbo.flights.dest AND dbo.flights.origin = 'JFK' GROUP BY origin, result_table_dest " +
                                "Declare @jfk as varchar(MAX) = (SELECT REPLACE(REPLACE((SELECT JFK = STUFF((SELECT ', ' + cast(JFK as nvarchar(20)) FROM  @LOCAL_TABLEVARIABLE " +
                                "AS x2 WHERE origin = x.origin ORDER BY origin FOR XML PATH('')), 1, 1, '') FROM @LOCAL_TABLEVARIABLE AS x GROUP BY origin " +
                                "ORDER BY origin FOR JSON PATH),'[{\"JFK\":\"','{\"JFK\":['),'\"}]','],')) Declare @dest as varchar(MAX) = (SELECT REPLACE(REPLACE(( SELECT dest = STUFF((SELECT '\",\"' + dest FROM  @LOCAL_TABLEVARIABLE " +
                                "AS x2 WHERE origin = x.origin ORDER BY origin FOR XML PATH('')), 1, 2, '') FROM @LOCAL_TABLEVARIABLE AS x GROUP BY origin " +
                                "ORDER BY origin FOR JSON PATH),'[{\"dest\":\"','\"dest\":['),'\"}]','\"],')) DECLARE @EWR_table  table(origin varchar(200), EWR int) " +
                                "INSERT INTO @EWR_table SELECT origin, COUNT(*) AS EWR FROM(SELECT TOP 10 dest AS result_table_dest, count(*) AS number_of_flights " +
                                "FROM dbo.flights GROUP BY dest ORDER BY number_of_flights DESC) AS result_table INNER JOIN dbo.flights " +
                                "ON result_table.result_table_dest = dbo.flights.dest AND dbo.flights.origin = 'EWR' GROUP BY origin, result_table_dest " + 
                                "Declare @ewr as varchar(MAX) = (SELECT REPLACE(REPLACE((SELECT EWR = STUFF((SELECT ', ' + cast(EWR as nvarchar(20)) " +
                                "FROM  @EWR_table " +
                                "AS x2 WHERE origin = x.origin ORDER BY origin FOR XML PATH('')), 1, 1, '') " +
                                "FROM @EWR_table AS x GROUP BY origin ORDER BY origin FOR JSON PATH),'[{\"EWR\":\"','\"EWR\":['),'\"}]','],')) " +
                                "DECLARE @LGA_table  table(origin varchar(200), LGA int) INSERT INTO @LGA_table " +
                                "SELECT origin, COUNT(*) AS JFK FROM(SELECT TOP 10 dest AS result_table_dest, count(*) AS number_of_flights " +
                                "FROM dbo.flights GROUP BY dest ORDER BY number_of_flights DESC) AS result_table " +
                                "INNER JOIN dbo.flights ON result_table.result_table_dest = dbo.flights.dest AND dbo.flights.origin = 'LGA' " +
                                "GROUP BY origin, result_table_dest Declare @lga as varchar(MAX) = (SELECT REPLACE(REPLACE((SELECT LGA = STUFF((SELECT ', ' + cast(LGA as nvarchar(20)) " +
                                "FROM  @LGA_table AS x2 WHERE origin = x.origin ORDER BY origin FOR XML PATH('')), 1, 1, '') " +
                                "FROM @LGA_table AS x GROUP BY origin ORDER BY origin FOR JSON PATH),'[{\"LGA\":\"','\"LGA\":['),'\"}]',']}')) Select CONCAT(@jfk, @dest, @ewr, @lga); ";
                            break;
                        /*Mean(Average) airtime for each origin */
                        case "avg-airtime": dbQuery = "SELECT origin, AVG(airs) AS average_air_time FROM (SELECT origin,CAST(air_time AS decimal(18,2)) AS airs FROM dbo.flights " +
                                "WHERE ISNUMERIC(air_time) = 1 AND air_time IS NOT NULL AND(origin = 'JFK' OR origin = 'EWR' OR origin = 'LGA')) AS resultTable GROUP BY origin FOR JSON PATH" ;
                            break;
                        case "delays": dbQuery = "SELECT origin,AVG(ari_delay) AS average_ari_delay, AVG(dep_delay) AS average_dep_delay  FROM (SELECT origin, CAST(arr_delay AS decimal) " +
                                "AS ari_delay, CAST(dep_delay AS float) AS dep_delay FROM dbo.flights " +
                                "WHERE ISNUMERIC(dep_delay) = 1 AND ISNUMERIC(arr_delay) = 1 AND dep_delay IS NOT NULL AND arr_delay IS NOT NULL AND(origin = 'JFK' OR origin = 'EWR' OR origin = 'LGA')) AS resultTable " +
                                "GROUP BY origin FOR JSON PATH;";
                            break;
                        default: dbQuery = "error";
                            break;
                    }

                    if (dbQuery != "error" || dbQuery != "")
                    {
                        using (SqlCommand cmd = new SqlCommand(dbQuery, conn))
                        {
                            SqlDataReader reader = await cmd.ExecuteReaderAsync();
                            // Execute the command and log the # rows affected.
                            while (reader.Read())
                            {
                                IDataRecord result = (IDataRecord)reader;
                                responseMessage += String.Format("{0}", result[0]);
                            }
                            // Call Close when done reading.
                            reader.Close();

                        }
                    }
                    else
                    {
                        return new NotFoundObjectResult(userRequest);
                    }

                }
                if(userRequest =="top-dest")
                {
                    responseMessage = responseMessage.Replace("\\", "");
                }
                return new OkObjectResult(responseMessage);
            }
            else
            {
                return new NotFoundObjectResult(userRequest);
            }
        }
        private static void ReadSingleRow(IDataRecord record, ILogger log)
        {
            log.LogInformation(String.Format("{0}", record[0]));
        }
     
    }
}
