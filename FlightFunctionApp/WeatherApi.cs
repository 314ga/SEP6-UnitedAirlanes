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
            log.LogInformation("C# HTTP trigger function processed a request.");

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
                            text =  "SELECT COUNT(*) AS weather_obs_origin " +
                                    "FROM dbo.weather WHERE origin = 'JFK' "+
                                    "UNION ALL "+
                                    "SELECT COUNT(*) AS weather_obs_origin2 "+
                                    "FROM dbo.weather WHERE origin = 'EWR' "+
                                    "UNION ALL "+
                                    "SELECT COUNT(*) AS weather_obs_origin3 "+
                                    "FROM dbo.weather WHERE origin = 'LGA' FOR JSON PATH;";
                            break;
                        }
                        case "temp-attributes":
                        {
                            text = "SELECT origin,CAST(temp AS float) AS temp FROM dbo.weather "+
                                    "WHERE origin = 'JFK' AND ISNUMERIC(temp) = 1 UNION ALL "+
                                   "SELECT origin, CAST(temp AS float) AS temp FROM dbo.weather "+
                                   "WHERE origin = 'EWR'AND ISNUMERIC(temp) = 1 UNION ALL "+
                                   "SELECT origin, CAST(temp AS float) AS temp FROM dbo.weather " +
                                   "WHERE origin = 'LGA' AND ISNUMERIC(temp) = 1 FOR JSON PATH;";
                            break;
                        }
                        case "temp-jfk":
                        {
                            text = "SELECT origin,CAST(temp AS float) AS temp " +
                                    "FROM dbo.weather WHERE origin = 'JFK' AND ISNUMERIC(temp) = 1 FOR JSON PATH;";
                            break;
                        }
                        case "avgtemp-jfk":
                        {
                                text = "SELECT origin,datepart(day,time_hour) AS div_day,datepart(month,time_hour) " +
                                "AS div_month,datepart(year,time_hour) AS div_year, AVG(CAST(temp AS float)) AS temp  FROM dbo.weather " +
                                "WHERE ISNUMERIC(temp) = 1 AND temp IS NOT NULL AND origin = 'JFK' " +
                                "GROUP BY datepart(day, time_hour),datepart(month, time_hour),datepart(year, time_hour), " +
                                "origin ORDER BY origin,div_year,div_month,div_day ASC FOR JSON PATH;";
                            break;
                        }
                        case "avgtemp-origin":
                        {
                            text = "SELECT origin,datepart(day,time_hour) AS div_day,datepart(month,time_hour) " +
                                "AS div_month,datepart(year,time_hour) AS div_year, AVG(CAST(temp AS float)) AS temp  FROM dbo.weather " +
                                "WHERE ISNUMERIC(temp) = 1 AND temp IS NOT NULL AND origin = 'JFK' " +
                                "GROUP BY datepart(day, time_hour),datepart(month, time_hour),datepart(year, time_hour), origin " +
                                "UNION ALL SELECT origin,datepart(day, time_hour) AS div_day, datepart(month, time_hour) AS div_month, " +
                                "datepart(year, time_hour) AS div_year, AVG(CAST(temp AS float)) AS temp  FROM dbo.weather " +
                                "WHERE ISNUMERIC(temp) = 1 AND temp IS NOT NULL AND origin = 'EWR' " +
                                "GROUP BY datepart(day, time_hour),datepart(month, time_hour),datepart(year, time_hour), origin UNION ALL " +
                                "SELECT origin,datepart(day, time_hour) AS div_day, datepart(month, time_hour) AS div_month, " +
                                "datepart(year, time_hour) AS div_year, AVG(CAST(temp AS float)) AS temp  FROM dbo.weather " +
                                "WHERE ISNUMERIC(temp) = 1 AND temp IS NOT NULL AND origin = 'LGA' " +
                                "GROUP BY datepart(day, time_hour),datepart(month, time_hour),datepart(year, time_hour), origin " +
                                "ORDER BY origin, div_year, div_month, div_day ASC FOR JSON PATH; ";
                            break;
                        }
                        default:
                        {
                            text = "error";
                            break;
                        }
                        
                    }
                    if(text != "error" || text != "")
                    {
                        using (SqlCommand cmd = new SqlCommand(text, conn))
                        {
                            SqlDataReader reader = await cmd.ExecuteReaderAsync();
                            // Execute the command and log the # rows affected.
                            while (reader.Read())
                            {
                                IDataRecord result = (IDataRecord)reader;
                                responseMessage += String.Format("{0},", result[0]);
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
