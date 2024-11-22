using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace FMSoftlab.DataAccess
{
    public interface IExecutionContext
    {
        int CommandTimeout { get; set; }
        string ConnectionString { get; }
        IsolationLevel IsolationLevel { get; set; }
        bool LogServerMessages { get; set; }
        bool BeginTransaction { get; set; }
    }

    public class ExecutionContext : IExecutionContext
    {
        public string ConnectionString { get; set; }
        public int CommandTimeout { get; set; }
        public IsolationLevel IsolationLevel { get; set; }
        public bool LogServerMessages { get; set; }
        public bool BeginTransaction { get; set; }
        public ExecutionContext(string connectionString, int commandTimeout, IsolationLevel isolationLevel, bool beginTransaction)
        {
            CommandTimeout=commandTimeout;
            ConnectionString=connectionString;
            IsolationLevel=isolationLevel;
            BeginTransaction=beginTransaction;
        }

        public ExecutionContext(string connectionString, int commandTimeout, IsolationLevel isolationLevel) : this(connectionString, commandTimeout, isolationLevel, true)
        {

        }
        public ExecutionContext(string connectionString) : this(connectionString, 30, IsolationLevel.ReadCommitted, true)
        {

        }
    }
    public interface IExecutionContextFactory
    {
        IExecutionContext GetShortRunning();
        IExecutionContext TenSeconds();
        IExecutionContext ThirtySeconds();
        IExecutionContext OneMinute();
        IExecutionContext FiveMinutes();
        IExecutionContext TenMinutes();
        IExecutionContext OneHour();
        IExecutionContext TwoHours();
        IExecutionContext ThreeHours();
        IExecutionContext GetLongRunning();
        IExecutionContext GetForeverRunning();
    }

    public class ExecutionContextFactory : IExecutionContextFactory
    {
        private string _connectionString;
        private readonly IsolationLevel _isolationLevel;
        private readonly int _shortRunning;
        private readonly int _longrunning;

        public ExecutionContextFactory(string connectionString, IsolationLevel isolationLevel, int shortRunning, int longrunning)
        {
            _connectionString=connectionString;
            _isolationLevel=isolationLevel;
            _shortRunning=shortRunning;
            _longrunning=longrunning;
        }
        public IExecutionContext GetShortRunning()
        {
            return new ExecutionContext(_connectionString, _shortRunning, _isolationLevel);
        }

        public IExecutionContext TenSeconds()
        {
            return new ExecutionContext(_connectionString, 10, _isolationLevel);
        }
        public IExecutionContext ThirtySeconds()
        {
            return new ExecutionContext(_connectionString, 10, _isolationLevel);
        }

        public IExecutionContext OneMinute()
        {
            return new ExecutionContext(_connectionString, 60, _isolationLevel);
        }

        public IExecutionContext FiveMinutes()
        {
            return new ExecutionContext(_connectionString, 300, _isolationLevel);
        }

        public IExecutionContext TenMinutes()
        {
            return new ExecutionContext(_connectionString, 600, _isolationLevel);
        }
        public IExecutionContext OneHour()
        {
            return new ExecutionContext(_connectionString, 3600, _isolationLevel);
        }
        public IExecutionContext TwoHours()
        {
            return new ExecutionContext(_connectionString, 7200, _isolationLevel);
        }

        public IExecutionContext ThreeHours()
        {
            return new ExecutionContext(_connectionString, 10800, _isolationLevel);
        }

        public IExecutionContext GetLongRunning()
        {
            return new ExecutionContext(_connectionString, _longrunning, _isolationLevel);
        }
        public IExecutionContext GetForeverRunning()
        {
            return new ExecutionContext(_connectionString, 0, _isolationLevel);
        }
    }

}
