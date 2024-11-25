using Dapper;
using FmSoftlab.Logging;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace FMSoftlab.DataAccess
{
    public interface ISingleTransactionManager : IDisposable
    {
        void Commit();
        void Rollback();
        IDbTransaction BeginTransaction();
        Task<IDbTransaction> BeginTransactionAsync();
        Task Execute(string sql, DynamicParameters dynamicParameters, Func<IDbConnection, IDbTransaction, Task> execute);
    }
    public class SingleTransactionManager : ISingleTransactionManager
    {
        private IDbTransaction _tranaction;
        private ISqlConnectionProvider _connectionProvider;
        private IExecutionContext _executionContext;
        private ILogger _log;

        public SingleTransactionManager(ISqlConnectionProvider connectionProvider, IExecutionContext executionContext, ILogger log)
        {
            _connectionProvider=connectionProvider;
            _executionContext =executionContext;
            _tranaction=null;
            _log =log;
        }
        public IDbTransaction BeginTransaction()
        {
            if (_tranaction==null)
            {
                _tranaction=_connectionProvider.BeginTransaction(_executionContext.IsolationLevel);
            }
            return _tranaction;
        }
        public async Task<IDbTransaction> BeginTransactionAsync()
        {
            if (_tranaction==null)
            {
                _tranaction=await _connectionProvider.BeginTransactionAsync(_executionContext.IsolationLevel);
            }
            return _tranaction;
        }
        public void Commit()
        {
            if (_tranaction==null)
            {
                return;
            }
            try
            {
                try
                {
                    _tranaction.Commit();
                    _log?.LogInformation("Transaction Committed!");
                }
                finally
                {
                    try
                    {
                        _tranaction.Dispose();
                    }
                    finally
                    {
                        _tranaction=null;
                    }
                }
            }
            catch (Exception ex)
            {
                _log?.LogAllErrors(ex);
            }
        }
        public void Rollback()
        {
            if (_tranaction==null)
            {
                return;
            }
            try
            {
                try
                {
                    _tranaction.Rollback();
                    _log?.LogWarning("Transaction rollbacked");
                }
                finally
                {
                    try
                    {
                        _tranaction.Dispose();
                    }
                    finally
                    {
                        _tranaction=null;
                    }
                }
            }
            catch (Exception ex)
            {
                _log?.LogAllErrors(ex);
            }
        }
        public async Task Execute(string sql, DynamicParameters dynamicParameters, Func<IDbConnection, IDbTransaction, Task> execute)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return;
            try
            {
                await BeginTransactionAsync();
                await execute(_connectionProvider.Connection, _tranaction);
                Commit();
            }
            catch (Exception ex)
            {
                Rollback();
                string tracesqltext = SqlHelperUtils.BuildFinalQuery(sql, dynamicParameters);
                _log?.LogAllErrors(ex, tracesqltext);
                throw;
            }
        }
        public void Dispose()
        {
            Rollback();
        }
    }
    public static class SqlHelperUtils
    {
        public static string BuildFinalQuery(string commandText, DynamicParameters parameters)
        {
            if (string.IsNullOrWhiteSpace(commandText))
                throw new ArgumentNullException(nameof(commandText), "Command text cannot be null or empty.");

            var sb = new StringBuilder();

            if (parameters != null)
            {
                foreach (var paramName in parameters.ParameterNames)
                {
                    object value = parameters.Get<object>(paramName);
                    string sqlType = GetSqlType(value);
                    string formattedValue = FormatValue(value);
                    sb.AppendLine($"DECLARE @{paramName} {sqlType} = {formattedValue};");
                }
            }
            sb.AppendLine();
            sb.AppendLine(commandText);
            return sb.ToString();
        }

        private static string GetSqlType(object value)
        {
            if (value == null)
                return "NVARCHAR(MAX)";

            if (value is int)
                return "INT";
            if (value is long)
                return "BIGINT";
            if (value is short)
                return "SMALLINT";
            if (value is byte)
                return "TINYINT";
            if (value is bool)
                return "BIT";
            if (value is decimal)
                return "DECIMAL(18, 4)";
            if (value is double)
                return "FLOAT";
            if (value is float)
                return "REAL";
            if (value is DateTime)
                return "DATETIME";
            if (value is string)
                return "NVARCHAR(MAX)";

            return "NVARCHAR(MAX)"; // Fallback type
        }
        private static string FormatValue(object value)
        {
            if (value == null || value == DBNull.Value)
                return "NULL";

            if (value is string s)
                return $"'{s.Replace("'", "''")}'"; // Escape single quotes
            if (value is DateTime dt)
                return $"'{dt:yyyy-MM-dd HH:mm:ss.fff}'"; // Format DateTime
            if (value is bool b)
                return b ? "1" : "0"; // Convert boolean to SQL bit
            if (IsNumeric(value))
                return value.ToString(); // Leave numeric types as-is

            return $"'{value.ToString().Replace("'", "''")}'"; // Fallback for unknown types
        }
        private static bool IsNumeric(object value)
        {
            if (value == null)
                return false;

            return value is byte
                || value is sbyte
                || value is short
                || value is ushort
                || value is int
                || value is uint
                || value is long
                || value is ulong
                || value is float
                || value is double
                || value is decimal;
        }
    }
}