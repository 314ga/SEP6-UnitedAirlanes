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
    public static class FlightsPerMonth
    {
        [FunctionName("FlightsPerMonth")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            string responseMessage = string.IsNullOrEmpty(name)
                ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                : $"Hello, {name}. This HTTP triggered function executed successfully.";
            // Get the connection string from app settings and use it to create a connection.
            var str = Environment.GetEnvironmentVariable("sqldb_connection");
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                var text = "SELECT TOP (5) [carrier] ,[name] FROM[dbo].[airlines]";

                using (SqlCommand cmd = new SqlCommand(text, conn))
                {
                    SqlDataReader reader = await cmd.ExecuteReaderAsync();
                    // Execute the command and log the # rows affected.
                    while (reader.Read())
                    {
                        ReadSingleRow((IDataRecord)reader, log);
                    }

                    // Call Close when done reading.
                    reader.Close();
                    /* var rows = await cmd.ExecuteNonQueryAsync();
                     log.LogInformation($"{rows} rows were updated");*/
                }
            }
            return new OkObjectResult(responseMessage);
        }
        private static void ReadSingleRow(IDataRecord record, ILogger log)
        {
            log.LogInformation(String.Format("{0}, {1}", record[0], record[1]));
        }
    }
}
