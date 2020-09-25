using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;


namespace BlTools.PostgreFluentSqlWrapper
{
    public static class DataReaderExtensions
    {
        public static int GetInt32(this IDataReader reader, string name)
        {
            return reader.GetInt32(reader.GetOrdinal(name));
        }

        public static int? GetInt32Null(this IDataReader reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (int?)null : reader.GetInt32(ordinal);
        }

        public static long GetInt64(this IDataReader reader, string name)
        {
            return reader.GetInt64(reader.GetOrdinal(name));
        }

        public static long? GetInt64Null(this IDataReader reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (long?)null : reader.GetInt64(ordinal);
        }

        public static double GetDouble(this IDataReader reader, string name)
        {
            return reader.GetDouble(reader.GetOrdinal(name));
        }

        public static double? GetDoubleNull(this IDataReader reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (double?)null : reader.GetDouble(ordinal);
        }

        public static float GetFloat(this IDataReader reader, string name)
        {
            return reader.GetFloat(reader.GetOrdinal(name));
        }

        public static float? GetFloatNull(this IDataReader reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (float?)null : reader.GetFloat(ordinal);
        }

        public static bool GetBoolean(this IDataReader reader, string name)
        {
            return reader.GetBoolean(reader.GetOrdinal(name));
        }

        public static bool? GetBooleanNull(this IDataReader reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (bool?)null : reader.GetBoolean(ordinal);
        }

        public static string GetString(this IDataReader reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        public static DateTime GetDateTime(this IDataReader reader, string name)
        {
            return reader.GetDateTime(reader.GetOrdinal(name));
        }

        public static DateTime? GetDateTimeNull(this IDataReader reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (DateTime?)null : reader.GetDateTime(ordinal);
        }

        public static DateTime GetDateTimeUtc(this IDataReader reader, string name)
        {
            return DateTime.SpecifyKind(reader.GetDateTime(name), DateTimeKind.Utc);
        }

        public static DateTime? GetDateTimeUtcNull(this IDataReader reader, string name)
        {
            var result = reader.GetDateTimeNull(name);
            return !result.HasValue ? (DateTime?)null : DateTime.SpecifyKind(result.Value, DateTimeKind.Utc);
        }

        public static DateTimeOffset GetDateTimeOffset(this IDataReader reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return (DateTimeOffset)reader.GetValue(ordinal);
        }

        public static DateTimeOffset? GetDateTimeOffsetNull(this IDataReader reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (DateTimeOffset?)null : (DateTimeOffset)reader.GetValue(ordinal);
        }

        public static List<string> GetListString(this IDataReader reader, string name)
        {
            var arrayValues = (string[])reader.GetValue(reader.GetOrdinal(name));
            return arrayValues.ToList();
        }

        public static byte[] GetByteArray(this IDataReader reader, string name)
        {
            var arrayValues = (byte[])reader.GetValue(reader.GetOrdinal(name));
            return arrayValues;
        }
    }
}