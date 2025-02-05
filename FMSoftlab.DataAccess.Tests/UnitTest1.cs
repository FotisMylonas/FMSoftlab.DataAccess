using Dapper;
using FMSoftlab.DataAccess;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Extensions.Logging;
using System.Data;
using ExecutionContext = FMSoftlab.DataAccess.ExecutionContext;
namespace FMSoftlab.DataAccess.Tests
{
    public class DataAccessUnitTests
    {
        private readonly ILogger<DataAccessUnitTests> _logger;
        private readonly int CheckValue = 123456;
        private readonly string _connectionString = @"Server=(localdb)\MSSQLLocalDB;Database=tempdb;Trusted_Connection=True;TrustServerCertificate=True";

        public DataAccessUnitTests()
        {
            var logger = new LoggerConfiguration()
          .MinimumLevel.Verbose()
          .WriteTo.Console()
          .WriteTo.File(@"c:\temp\dataaccess.log", rollingInterval: RollingInterval.Day)
          .CreateLogger();

            var loggerFactory = new SerilogLoggerFactory(logger);
            _logger = loggerFactory.CreateLogger<DataAccessUnitTests>();
        }

        [Fact]
        public async Task Select_ExecuteScalar_1()
        {
            int id = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            using (ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString, _logger))
            {
                using (SingleTransactionManager tm = new SingleTransactionManager(con, context, _logger))
                {
                    tm.BeginTransaction();
                    DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
                    SqlExecution execution = new SqlExecution(context, tm, "Select @Id as Id", dyn, CommandType.Text, _logger);
                    id = await execution.ExecuteScalar<int>();
                    tm.Rollback();
                }
            }
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_ExecuteScalar_2()
        {
            ExecutionContext context = new ExecutionContext(_connectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            SqlExecution execution = new SqlExecution(context, "Select @Id as Id", dyn, CommandType.Text, _logger);
            int id = await execution.ExecuteScalar<int>();
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_Query_1()
        {
            int id = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            using ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString, _logger);
            using SingleTransactionManager tm = new SingleTransactionManager(con, context, _logger);

            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            tm.BeginTransaction();
            SqlExecution execution = new SqlExecution(context, tm, "Select @Id as Id", dyn, CommandType.Text, _logger);
            id = (await execution.Query<int>()).FirstOrDefault();
            tm.Rollback();


            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_Query_2()
        {
            ExecutionContext context = new ExecutionContext(_connectionString);
            ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString, _logger);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            SqlExecution execution = new SqlExecution(context, "Select @Id as Id", dyn, CommandType.Text, _logger);
            int id = (await execution.Query<int>()).FirstOrDefault();
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_FirstOrDefault_1()
        {
            ExecutionContext context = new ExecutionContext(_connectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            SqlExecution execution = new SqlExecution(context, "Select @Id as Id", dyn, CommandType.Text, _logger);
            int id = await execution.FirstOrDefault<int>();
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_FirstOrDefault_2()
        {
            int id = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            using (ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString, _logger))
            {
                using (SingleTransactionManager tm = new SingleTransactionManager(con, context, _logger))
                {
                    tm.BeginTransaction();
                    DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
                    SqlExecution execution = new SqlExecution(context, tm, "Select @Id as Id", dyn, CommandType.Text, _logger);
                    id = await execution.FirstOrDefault<int>();
                    tm.Rollback();
                }
            }
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Select_QueryMultiple_1()
        {
            int id = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString, _logger);
            SingleTransactionManager tm = new SingleTransactionManager(con, context, _logger);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            tm.BeginTransaction();
            SqlExecution execution = new SqlExecution(context, tm, "Select @Id as Id", dyn, CommandType.Text, _logger);
            await execution.QueryMultiple(async (x) =>
            {
                var dr = await x.ReadAsync<int>(buffered: false);
                id=dr.FirstOrDefault<int>();
            });
            tm.Rollback();
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Execute_2()
        {
            ExecutionContext context = new ExecutionContext(_connectionString);
            ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString, _logger);
            using SingleTransactionManager tm = new SingleTransactionManager(con, context, _logger);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            tm.BeginTransaction();
            SqlExecution execution = new SqlExecution(context, tm, "Select @Id as Id", dyn, CommandType.Text, _logger);
            await execution.Execute();
            tm.Rollback();
        }

        [Fact]
        public async Task Select_QueryMultiple_2()
        {
            int id = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            SqlExecution execution = new SqlExecution(context, "Select @Id as Id", dyn, CommandType.Text, _logger);
            await execution.QueryMultiple(async (x) =>
            {
                var dr = await x.ReadAsync<int>(buffered: false);
                id=dr.FirstOrDefault<int>();
            });
            Assert.Equal(CheckValue, id);
        }

        [Fact]
        public async Task Execute_1()
        {
            int id1 = 0;
            int id2 = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            using ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString, _logger);
            using SingleTransactionManager tm = new SingleTransactionManager(con, context, _logger);
            tm.BeginTransaction();
            await new SqlExecution(context, tm, "create table ##temptable(Id int);insert into ##temptable(Id) values(10);", _logger, CommandType.Text, _logger).Execute();
            id1 = await new SqlExecution(context, tm, "select id from ##temptable", null, CommandType.Text, _logger).FirstOrDefault<int>();
            await new SqlExecution(context, tm, "update t set t.id=@Id from ##temptable t;", dyn, CommandType.Text, _logger).Execute();
            id2 = await new SqlExecution(context, tm, "select t.id from ##temptable t where id=@Id", dyn, CommandType.Text, _logger).FirstOrDefault<int>();
            Assert.Equal(10, id1);
            Assert.Equal(CheckValue, id2);
        }

        [Fact]
        public async Task Execute_ConnectionPool()
        {
            for (int i = 0; i<1000; i++)
            {
                await Execute_1();
            }
        }

        [Fact]
        public async Task Execute_Reader()
        {
            int id1 = 0;
            ExecutionContext context = new ExecutionContext(_connectionString);
            DynamicParameters dyn = new DynamicParameters(new { Id = CheckValue });
            using ISqlConnectionProvider con = new SqlConnectionProvider(context.ConnectionString, _logger);
            using SingleTransactionManager tm = new SingleTransactionManager(con, context, _logger);
            tm.BeginTransaction();
            await new SqlExecution(context, tm, "create table ##temptable(Id int);insert into ##temptable(Id) values(10);", null, CommandType.Text, _logger).Execute();
            using (IDataReader read = await new SqlExecution(context, tm, "select id from ##temptable", null, CommandType.Text, _logger).ExecuteReader())
            {
                while (read.Read())
                {
                    id1 = read.GetInt32(0);
                }
            }
            tm.Rollback();
            Assert.Equal(10, id1);
        }
    }
}