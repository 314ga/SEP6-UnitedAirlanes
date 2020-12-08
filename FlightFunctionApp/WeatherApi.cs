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

namespace WeatherFunctionApp
{
    public static class WeatherApi
    {
        [FunctionName("WeatherApi")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("API for United Airlines");

            string userRequest = req.Query["requestBody"];
            string responseMessage = "";
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            userRequest = userRequest ?? data?.requestBody;

            if(!string.IsNullOrEmpty(userRequest))
            {
                var str = Environment.GetEnvironmentVariable("sqldb_connection");
                using (SqlConnection conn = new SqlConnection(str))
                {
                    var text = "";
                    conn.Open();
                    switch(userRequest)
                    {
                        case "wo-origins":
                        {
                            text =  "SELECT origin, COUNT(*) AS weather_obs_origin " +
                                    "FROM dbo.weather WHERE origin = 'JFK' GROUP BY origin " +
                                    "UNION ALL "+
                                    "SELECT origin, COUNT(*) AS weather_obs_origin2 " +
                                    "FROM dbo.weather WHERE origin = 'EWR' GROUP BY origin " +
                                    "UNION ALL "+
                                    "SELECT origin, COUNT(*) AS weather_obs_origin3 " +
                                    "FROM dbo.weather WHERE origin = 'LGA' GROUP BY origin FOR JSON PATH;";
                            break;
                        }
                        case "temp-attributes":
                        {
                                text = "Declare @jfk as varchar(MAX)= (SELECT CONCAT(year,'-' , month , '-' , day, 'T',hour , ':00:00Z') AS x, (CAST(temp AS float)-32)/1.8 AS y, " +
                                    "CAST(2 AS int) AS r  FROM dbo.weather WHERE origin = 'JFK' AND ISNUMERIC(temp) = 1  FOR JSON PATH) " +
                                    "Declare @ewr as varchar(MAX) = (SELECT CONCAT(year,'-' , month , '-' , day, 'T',hour , ':00:00Z') AS x,  (CAST(temp AS float)-32)/1.8 AS y, " +
                                    "CAST(2 AS int) AS r  FROM dbo.weather WHERE origin = 'EWR' AND ISNUMERIC(temp) = 1  FOR JSON PATH) " +
                                    "Declare @lga as varchar(MAX) = (SELECT CONCAT(year,'-' , month , '-' , day, 'T',hour , ':00:00Z') AS x, (CAST(temp AS float)-32)/1.8 AS y, CAST(2 AS int) " +
                                    "AS r  FROM dbo.weather WHERE origin = 'LGA' AND ISNUMERIC(temp) = 1  FOR JSON PATH) " +
                                    "Select  CONCAT('{\"JFK\":[',substring(@jfk,2,(LEN(@jfk)-2)), '],\"EWR\":[',substring" +
                                    "(@ewr,2,(LEN(@ewr)-2)), '],\"LGA\":[',substring(@lga,2,(LEN(@lga)-2)), ']}')";



                            break;
                        }
                        case "dewp-attributes":
                        {
                            text = "Declare @jfk as varchar(MAX)= (SELECT CONCAT(year,'-' , month , '-' , day, 'T',hour , ':00:00Z') AS x, (CAST(dewp AS float)-32)/1.8 AS y, " +
                                "CAST(2 AS int) AS r  FROM dbo.weather WHERE origin = 'JFK' AND ISNUMERIC(dewp) = 1  FOR JSON PATH) " +
                                "Declare @ewr as varchar(MAX) = (SELECT CONCAT(year,'-' , month , '-' , day, 'T',hour , ':00:00Z') AS x,  (CAST(dewp AS float)-32)/1.8 AS y, " +
                                "CAST(2 AS int) AS r  FROM dbo.weather WHERE origin = 'EWR' AND ISNUMERIC(dewp) = 1  FOR JSON PATH) " +
                                "Declare @lga as varchar(MAX) = (SELECT CONCAT(year,'-' , month , '-' , day, 'T',hour , ':00:00Z') AS x, (CAST(dewp AS float)-32)/1.8 AS y, CAST(2 AS int) " +
                                "AS r  FROM dbo.weather WHERE origin = 'LGA' AND ISNUMERIC(dewp) = 1  FOR JSON PATH) " +
                                "Select  CONCAT('{\"JFK\":[',substring(@jfk,2,(LEN(@jfk)-2)), '],\"EWR\":[',substring" +
                                "(@ewr,2,(LEN(@ewr)-2)), '],\"LGA\":[',substring(@lga,2,(LEN(@lga)-2)), ']}')";



                            break;
                        }
                        case "avgtemp-origin":
                        {
                            text = "Declare @jfk as varchar(MAX)= ( SELECT * FROM( SELECT CONCAT(datepart(day, time_hour), " +
                                    "'-' , datepart(month, time_hour),'-', datepart(year, time_hour)) AS x, AVG((CAST(dewp AS float)-32)/1.8) " +
                                    "AS y, CAST(4 AS int) AS r  FROM dbo.weather WHERE origin = 'JFK' AND ISNUMERIC(temp) = 1 " +
                                    "GROUP BY datepart(day, time_hour),datepart(month, time_hour),datepart(year, time_hour), origin) " +
                                    "AS t FOR JSON PATH) " +
                                    "Declare @ewr as varchar(MAX) = (SELECT * FROM( SELECT CONCAT(datepart(day, time_hour), '-', " +
                                    "datepart(month, time_hour), '-', datepart(year, time_hour)) AS x, AVG((CAST(dewp AS float)-32)/1.8) AS y, " +
                                    "CAST(4 AS int) AS r  FROM dbo.weather WHERE origin = 'EWR' AND ISNUMERIC(temp) = 1 GROUP BY datepart" +
                                    "(day, time_hour), datepart(month, time_hour), datepart(year, time_hour), origin) AS t FOR JSON PATH) " +
                                    "Declare @lga as varchar(MAX) = (SELECT * FROM( SELECT CONCAT(datepart(day, time_hour), '-', datepart" +
                                    "(month, time_hour), '-', datepart(year, time_hour)) AS x, AVG((CAST(dewp AS float)-32)/1.8) AS y, CAST(4 AS int) " +
                                    "AS r  FROM dbo.weather WHERE origin = 'LGA' AND ISNUMERIC(temp) = 1 GROUP BY datepart(day, time_hour), " +
                                    "datepart(month, time_hour), datepart(year, time_hour), origin) AS t FOR JSON PATH) Select CONCAT('{\"JFK\"" +
                                    ":[',substring(@jfk, 2, (LEN(@jfk) - 2)), '],\"EWR\":[',substring(@ewr, 2, (LEN(@ewr) - 2)), '],\"LGA\":['," +
                                    "substring(@lga, 2, (LEN(@lga) - 2)), ']}')";
                            break;
                        }
                        default:
                        {
                            text = "error";
                            break;
                        }
                        
                    }
                    if(text != "error" || text == "")
                    {
                        using (SqlCommand cmd = new SqlCommand(text, conn))
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
                return new OkObjectResult(responseMessage);
            }
            else
            {
                return new NotFoundObjectResult(userRequest);
            }
            // Get the connection string from app settings and use it to create a connection.
           
            
        }
    }
}
