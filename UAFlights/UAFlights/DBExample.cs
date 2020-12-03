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
using System.Threading.Tasks;
using System.Data;

namespace UAFlights
{
    public static class DBExample
    {
        [FunctionName("DBExample")]
        public static async Task Run([TimerTrigger("*/315 * * * * *")]TimerInfo myTimer, ILogger log)
        {
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
        }
        private static void ReadSingleRow(IDataRecord record, ILogger log)
        {
            log.LogInformation(String.Format("{0}, {1}", record[0], record[1]));
        }
    }
    
}
