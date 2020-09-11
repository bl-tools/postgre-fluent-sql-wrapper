using System.Data.Common;
using Npgsql;


namespace BlTools.PostgreFluentSqlWrapper
{
    public static class ConnectionStringHelper
    {
        public static DbConnectionStringBuilder CreateConnectionStringBuilder(string connectionString)
        {
            var result = new NpgsqlConnectionStringBuilder(connectionString);
            return result;
        }
    }
}