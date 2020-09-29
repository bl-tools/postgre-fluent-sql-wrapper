using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;


namespace BlTools.PostgreFluentSqlWrapper
{
    public class FluentSqlCommand
    {
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlCommand _command;
        private bool _needPreparation;
        private bool _needReloadTypes;
        private Action<IDataReader> _dataReaderAction;
        private Dictionary<string, Exception> _exceptionsOnConstraints;
        private bool _needToExecAsStoredProcedure = false;
        private string _refCursorName;


        public FluentSqlCommand(string connectionString)
        {
            _connection = new NpgsqlConnection(connectionString);
            _command = new NpgsqlCommand();
            _command.Connection = _connection;
            _needPreparation = true;
            _needReloadTypes = false;
        }
        
        /// <summary>
        /// Method creates a command for calling function. Will be generated statement like "select functionName..."
        /// </summary>
        /// <param name="functionName">Name of function</param>
        /// <returns></returns>
        public FluentSqlCommand Function(string functionName)
        {
            _command.CommandText = functionName;
            _command.CommandType = CommandType.StoredProcedure; //NpgSql currently doesn't support procedures.
            return this;
        }

        /// <summary>
        /// Raw sql command
        /// </summary>
        /// <param name="queryText"></param>
        /// <returns></returns>
        public FluentSqlCommand Query(string queryText)
        {
            _command.CommandText = queryText;
            _command.CommandType = CommandType.Text;
            return this;
        }

        /// <summary>
        /// Method creates command for calling stored procedures in PostgreDB
        /// </summary>
        /// <param name="procedureExecText">text of sql statement how procedure will be called. ex - "call procedureName(param1, param2...)</param>
        /// <param name="refCursorName">If stored procedure will return data need to set name of refcursor variable</param>
        /// <returns></returns>
        public FluentSqlCommand StoredProcedure(string procedureExecText, string refCursorName="")
        {
            _needToExecAsStoredProcedure = true;
            _refCursorName = refCursorName;
            _needPreparation = false;
            _command.CommandText = procedureExecText;
            _command.CommandType = CommandType.Text;
            return this;
        }

        public FluentSqlCommand WithTimeout(int commandTimeout)
        {
            if (commandTimeout < 0)
            {
                throw new ArgumentException("Command timeout should be >= 0");
            }
            _command.CommandTimeout = commandTimeout;
            return this;
        }

        public FluentSqlCommand WithoutPreparation()
        {
            _needPreparation = false;
            return this;
        }

        public FluentSqlCommand WithTypeReloading()
        {
            _needReloadTypes = true;
            return this;
        }

        public FluentSqlCommand ThrowOnConstraint<T>(string constraintName) where T : Exception, new()
        {
            if (_exceptionsOnConstraints == null)
            {
                _exceptionsOnConstraints = new Dictionary<string, Exception>(StringComparer.InvariantCultureIgnoreCase);
            }
            _exceptionsOnConstraints.Add(constraintName, new T());
            return this;
        }



        #region [ Add parameter ]

        public FluentSqlCommand AddParam(string name, string value)
        {
            if (value != null)
            {
                FillParam(name, value, NpgsqlDbType.Varchar);
            }
            else
            {
                FillNullParam(name, NpgsqlDbType.Varchar);
            }
            return this;
        }

        public FluentSqlCommand AddParam(string name, int? value)
        {
            if (value != null)
            {
                FillParam(name, value.Value, NpgsqlDbType.Integer);
            }
            else
            {
                FillNullParam(name, NpgsqlDbType.Integer);
            }
            return this;
        }

        public FluentSqlCommand AddParam(string name, float? value)
        {
            if (value != null)
            {
                FillParam(name, value.Value, NpgsqlDbType.Real);
            }
            else
            {
                FillNullParam(name, NpgsqlDbType.Real);
            }
            return this;
        }

        public FluentSqlCommand AddParam(string name, long? value)
        {
            if (value != null)
            {
                FillParam(name, value.Value, NpgsqlDbType.Bigint);
            }
            else
            {
                FillNullParam(name, NpgsqlDbType.Bigint);
            }
            return this;
        }

        public FluentSqlCommand AddParam(string name, DateTime? value)
        {
            if (value != null)
            {
                FillParam(name, value.Value, NpgsqlDbType.Timestamp);
            }
            else
            {
                FillNullParam(name, NpgsqlDbType.Timestamp);
            }
            return this;
        }

        public FluentSqlCommand AddParam(string name, bool? value)
        {
            if (value != null)
            {
                FillParam(name, value.Value, NpgsqlDbType.Boolean);
            }
            else
            {
                FillNullParam(name, NpgsqlDbType.Boolean);
            }
            return this;
        }

        public FluentSqlCommand AddParam(string name, IEnumerable<string> value)
        {
            if (value != null)
            {
                FillParam(name, value, NpgsqlDbType.Array | NpgsqlDbType.Varchar);
            }
            else
            {
                FillNullParam(name, NpgsqlDbType.Array | NpgsqlDbType.Varchar);
            }
            return this;
        }
        public FluentSqlCommand AddParam(string name, byte[] value)
        {
            if (value != null)
            {
                FillParam(name, value, NpgsqlDbType.Bytea);
            }
            else
            {
                FillNullParam(name, NpgsqlDbType.Bytea);
            }
            return this;
        }

        public FluentSqlCommand AddCompositeParam<T>(string name, T value, string sqlTypeName) where T : new()
        {
            InitCommand();
            _connection.TypeMapper.MapComposite<T>(sqlTypeName);
            _command.Parameters.AddWithValue(name, value);
            return this;
        }

        public FluentSqlCommand AddCompositeParamCollection<T>(string name, T[] values, string sqlTypeName) where T : new()
        {
            InitCommand();
            _connection.TypeMapper.MapComposite<T>(sqlTypeName);
            _command.Parameters.AddWithValue(name, values);
            return this;
        }

        private void FillParam<T>(string name, T value, NpgsqlDbType dbType)
        {
            var parameter = new NpgsqlParameter<T>(name, dbType);
            parameter.TypedValue = value;
            _command.Parameters.Add(parameter);
        }

        private void FillNullParam(string name, NpgsqlDbType dbType)
        {
            var parameter = new NpgsqlParameter(name, dbType);
            parameter.Value = DBNull.Value;
            _command.Parameters.Add(parameter);
        }

        public FluentSqlCommand AddJsonParam(string name, string value)
        {
            if (value != null)
            {
                FillParam(name, value, NpgsqlDbType.Json);
            }
            else
            {
                FillNullParam(name, NpgsqlDbType.Json);
            }
            return this;
        }

        #endregion


        #region [ Execute ]

        public void ExecNonQuery()
        {
            if (_needToExecAsStoredProcedure)
            {
                ExecStoredProcedure(ExecType.NonQuery);
            }
            else
            {
                Exec(ExecType.NonQuery);
            }
        }

        public async Task ExecNonQueryAsync()
        {
            if (_needToExecAsStoredProcedure)
            {
                await ExecStoredProcedureAsync(ExecType.NonQuery);
            }
            else
            {
                await ExecAsync(ExecType.NonQuery);
            }
        }


        public T ExecRead<T>(Func<IDataReader, T> itemBuilder)
        {
            var result = default(T);
            _dataReaderAction = reader => result = itemBuilder(reader);

            if (_needToExecAsStoredProcedure)
            {
                ExecStoredProcedure(ExecType.Reader);
            }
            else
            {
                Exec(ExecType.Reader);
            }

            return result;
        }

        public async Task<T> ExecReadAsync<T>(Func<IDataReader, T> itemBuilder)
        {
            var result = default(T);
            _dataReaderAction = reader => result = itemBuilder(reader);

            if (_needToExecAsStoredProcedure)
            {
                await ExecStoredProcedureAsync(ExecType.Reader);
            }
            else
            {
                await ExecAsync(ExecType.Reader);
            }

            return result;
        }


        public List<T> ExecReadList<T>(Func<IDataReader, T> itemBuilder)
        {
            var resultList = new List<T>();
            _dataReaderAction = reader => resultList.Add(itemBuilder(reader));

            if (_needToExecAsStoredProcedure)
            {
                ExecStoredProcedure(ExecType.Reader);
            }
            else
            {
                Exec(ExecType.Reader);
            }

            return resultList;
        }

        public async Task<List<T>> ExecReadListAsync<T>(Func<IDataReader, T> itemBuilder)
        {
            var resultList = new List<T>();
            _dataReaderAction = reader => resultList.Add(itemBuilder(reader));

            if (_needToExecAsStoredProcedure)
            {
                await ExecStoredProcedureAsync(ExecType.Reader);
            }
            else
            {
                await ExecAsync(ExecType.Reader);
            }
            return resultList;
        }


        public T ExecScalar<T>()
        {
            object rawResult;

            if (_needToExecAsStoredProcedure)
            {
                rawResult = ExecStoredProcedure(ExecType.Scalar);
            }
            else
            {
                rawResult = Exec(ExecType.Scalar);
            }
            var result = rawResult == DBNull.Value ? default(T) : (T)rawResult;
            return result;
        }

        public async Task<T> ExecScalarAsync<T>()
        {
            object rawResult;
            if (_needToExecAsStoredProcedure)
            {
                rawResult = await ExecStoredProcedureAsync(ExecType.Scalar);
            }
            else
            {
                rawResult = await ExecAsync(ExecType.Scalar);
            }
            var result = rawResult == DBNull.Value ? default(T) : (T)rawResult;
            return result;
        }

        #endregion


        private object Exec(ExecType execType)
        {
            object result = null;
            using (_command.Connection)
            {
                try
                {
                    InitCommand();
                    using (_command)
                    {
                        if (_needPreparation)
                        {
                            _command.Prepare();
                        }

                        if (_needReloadTypes)
                        {
                            _command.Connection?.ReloadTypes();
                        }

                        switch (execType)
                        {
                            case ExecType.NonQuery:
                                {
                                    _command.ExecuteNonQuery();
                                    break;
                                }
                            case ExecType.Reader:
                                {
                                    IDataReader reader = _command.ExecuteReader();
                                    using (reader)
                                    {
                                        do
                                        {
                                            while (reader.Read())
                                            {
                                                _dataReaderAction?.Invoke(reader);
                                            }
                                        }
                                        while (reader.NextResult());
                                    }

                                    break;
                                }
                            case ExecType.Scalar:
                            {
                                result = _command.ExecuteScalar();
                                break;
                            }
                        }
                    }
                }
                catch (PostgresException ex)
                {
                    ProcessException(ex);
                }
            }

            return result;
        }

        private async Task<object> ExecAsync(ExecType execType)
        {
            object result = null;
            using (_command.Connection)
            {
                try
                {
                    await InitCommandAsync();
                    using (_command)
                    {
                        if (_needPreparation)
                        {
                            await _command.PrepareAsync();
                        }

                        if (_needReloadTypes)
                        {
                            _command.Connection?.ReloadTypes();
                        }

                        switch (execType)
                        {
                            case ExecType.NonQuery:
                                {
                                    await _command.ExecuteNonQueryAsync();
                                    break;
                                }
                            case ExecType.Reader:
                                {
                                    var reader = await _command.ExecuteReaderAsync();
                                    using (reader)
                                    {
                                        do
                                        {
                                            while (await reader.ReadAsync())
                                            {
                                                _dataReaderAction?.Invoke(reader);
                                            }
                                        } while (await reader.NextResultAsync());
                                    }
                                    break;
                                }
                            case ExecType.Scalar:
                                {
                                    result = await _command.ExecuteScalarAsync();
                                    break;
                                }
                        }
                    }
                }
                catch (PostgresException ex)
                {
                    ProcessException(ex);
                }
            }

            return result;
        }

        #region exec stored procedures
        private object ExecStoredProcedure(ExecType execType)
        {
            object result = null;
            NpgsqlTransaction transaction = null;

            using (_command.Connection)
            {
                try
                {
                    InitCommand();
                    transaction = _command.Connection.BeginTransaction();

                    using (_command)
                    {
                        if (_needPreparation)
                        {
                            _command.Prepare();
                        }

                        if (_needReloadTypes)
                        {
                            _command.Connection?.ReloadTypes();
                        }

                        switch (execType)
                        {
                            case ExecType.NonQuery:
                            {
                                _command.ExecuteNonQuery();
                                break;
                            }
                            case ExecType.Reader:
                            {
                                _command.ExecuteNonQuery();

                                var fetchAllCommand = new NpgsqlCommand($"FETCH ALL IN \"{_refCursorName}\";", _command.Connection);
                                fetchAllCommand.Transaction = transaction;

                                IDataReader reader = fetchAllCommand.ExecuteReader();
                                using (reader)
                                {
                                    do
                                    {
                                        while (reader.Read())
                                        {
                                            _dataReaderAction?.Invoke(reader);
                                        }
                                    }
                                    while (reader.NextResult());
                                }

                                break;
                            }
                            case ExecType.Scalar:
                            {
                                _command.ExecuteNonQuery();

                                var fetchAllCommand = new NpgsqlCommand($"FETCH ALL IN \"{_refCursorName}\";", _command.Connection);
                                fetchAllCommand.Transaction = transaction;

                                result = fetchAllCommand.ExecuteScalar();
                                break;
                            }
                        }
                    }

                    transaction.Commit();
                }
                catch (PostgresException ex)
                {
                    transaction?.RollbackAsync();
                    ProcessException(ex);
                }
            }

            return result;
        }

        private async Task<object> ExecStoredProcedureAsync(ExecType execType)
        {
            object result = null;
            NpgsqlTransaction transaction = null;

            using (_command.Connection)
            {
                try
                {
                    await InitCommandAsync();
                    transaction = _command.Connection.BeginTransaction();

                    using (_command)
                    {
                        if (_needPreparation)
                        {
                            await _command.PrepareAsync();
                        }

                        if (_needReloadTypes)
                        {
                            _command.Connection?.ReloadTypes();
                        }

                        switch (execType)
                        {
                            case ExecType.NonQuery:
                                {
                                    await _command.ExecuteNonQueryAsync();
                                    break;
                                }
                            case ExecType.Reader:
                                {
                                    await _command.ExecuteNonQueryAsync();

                                    var fetchAllCommand = new NpgsqlCommand($"FETCH ALL IN \"{_refCursorName}\";", _command.Connection);
                                    fetchAllCommand.Transaction = transaction;

                                    var reader = await fetchAllCommand.ExecuteReaderAsync();
                                    using (reader)
                                    {
                                        do
                                        {
                                            while (await reader.ReadAsync())
                                            {
                                                _dataReaderAction?.Invoke(reader);
                                            }
                                        }
                                        while (await reader.NextResultAsync());
                                    }

                                    break;
                                }
                            case ExecType.Scalar:
                                {
                                    await _command.ExecuteNonQueryAsync();

                                    var fetchAllCommand = new NpgsqlCommand($"FETCH ALL IN \"{_refCursorName}\";", _command.Connection);
                                    fetchAllCommand.Transaction = transaction;

                                    result = await fetchAllCommand.ExecuteScalarAsync();
                                    break;
                                }
                        }
                    }

                    transaction.Commit();
                }
                catch (PostgresException ex)
                {
                    transaction?.RollbackAsync();
                    ProcessException(ex);
                }
            }

            return result;
        }
        #endregion exec stored procedures


        private void InitCommand()
        {
            if (_command.Connection.State != ConnectionState.Open)
            {
                _command.Connection.Open();
            }
        }

        private async Task InitCommandAsync()
        {
            if (_command.Connection.State != ConnectionState.Open)
            {
                await _command.Connection.OpenAsync();
            }
        }

        private void ProcessException(PostgresException ex)
        {
            var constraintName = ex.ConstraintName ?? string.Empty;
            if (_exceptionsOnConstraints != null && _exceptionsOnConstraints.ContainsKey(constraintName))
            {
                var exception = _exceptionsOnConstraints[constraintName];
                throw exception;
            }

            throw ex;
        }

        private enum ExecType
        {
            NonQuery = 0,
            Reader = 1,
            Scalar = 2
        }
    }
}