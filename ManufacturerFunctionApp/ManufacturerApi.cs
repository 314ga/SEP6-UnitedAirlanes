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

namespace ManufacturerFunctionApp
{
    public static class ManufacturerApi
    {
        [FunctionName("ManufacturerApi")]
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

            if (!string.IsNullOrEmpty(userRequest))
            {
                var str = Environment.GetEnvironmentVariable("sqldb_connection");
                using (SqlConnection conn = new SqlConnection(str))
                {
                    var text = "";
                    conn.Open();
                    switch (userRequest)
                    {
                        case "a":
                            {
                                text = "aaaaaa";
                                break;
                            }
                        case "b":
                            {
                                text = "bbbbbb";
                                break;
                            }
                        case "c":
                            {
                                text = "ccccccc";
                                break;
                            }
                        case "d":
                            {
                                text = "dddddddd";
                                break;
                            }
                        case "e":
                            {
                                text = "eeeee";
                                break;
                            }
                        default:
                            {
                                text = "error";
                                break;
                            }

                    }
                    if (text != "error" || text != "")
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
