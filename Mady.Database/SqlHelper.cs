using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Mady.Database
{
	public class SqlHelper : IDisposable
	{
		private const int MAX_PARAMETERS = 2100;
		private const int MAX_ROWS = 1000;

		/// <summary>
		/// The timeout in seconds for any commands run with this helper.
		/// </summary>
		public int CommandTimeout { get; set; }

		private SqlConnection Connection { get; set; }
		private SqlTransaction Transaction { get; set; }

		/// <summary>
		/// Creates and opens a new SQL connection from the specified connection string.
		/// </summary>
		/// <param name="connectionString">The connection string of the database to connect to.</param>
		public SqlHelper(string connectionString, int commandTimeout = 120)
		{
			CommandTimeout = commandTimeout;
			Connection = new SqlConnection(connectionString);
			Connection.Open();
		}

		/// <summary>
		/// Disposes of the transaction and connection if necessary.
		/// </summary>
		public void Dispose()
		{
			Transaction?.Dispose();
			Connection.Dispose();
		}

		/// <summary>
		/// Begins a new transaction, disposing (and rolling back) the previous transaction
		/// if it was still in progress.
		/// </summary>
		public SqlHelper BeginTransaction()
		{
			Transaction?.Dispose();
			Transaction = Connection.BeginTransaction();
			return this;
		}

		/// <summary>
		/// Commit the current SQL transaction.
		/// </summary>
		public SqlHelper CommitTransaction()
		{
			Transaction?.Commit();
			return this;
		}

		/// <summary>
		/// Creates a table-valued parameter given the name, items, and the named SQL type.
		/// </summary>
		/// <param name="name">The name of the parameter.</param>
		/// <param name="items">List of dictionaries of strings and objects containing the values for the TVP.</param>
		/// <param name="type">The named SQL type of the TVP.</param>
		/// <returns>The table-valued parameter.</returns>
		public static SqlParameter TableValuedParameter(string name, List<Dictionary<string, object>> items, string type)
		{
			var dt = new DataTable();
			foreach (var col in items[0])
			{
				dt.Columns.Add(col.Key, (col.Value ?? "").GetType());
			}
			foreach (var row in items)
			{
				var dr = dt.NewRow();
				foreach (var col in row) { dr[col.Key] = col.Value; }
				dt.Rows.Add(dr);
			}

			var param = new SqlParameter(name, dt)
			{
				SqlDbType = SqlDbType.Structured,
				TypeName = type
			};
			return param;
		}

		#region Command Builders

		/// <summary>
		/// Adds a value between 'SELECT' and the list of fields, such as 'TOP 100' to the current query.
		/// </summary>
		/// <param name="filter">Any value between 'SELECT' and the list of fields, such as 'TOP 100'</param>
		/// <returns>The new Query object.</returns>
		public Query Filter(string filter) => new Query(this).Filter(filter);

		/// <summary>
		/// The View, Table or Join to select from.
		/// </summary>
		/// <param name="from">The view, table or join to select from.</param>
		/// <returns>The new Query object.</returns>
		public Query From(string from) => new Query(this).From(from);

		/// <summary>
		/// The group by clause to apply to the current query.
		/// </summary>
		/// <param name="groupBy">The group by clause to apply.</param>
		/// <returns>The new Query object.</returns>
		public Query GroupBy(string groupBy) => new Query(this).GroupBy(groupBy);

		/// <summary>
		/// The having clause to apply to the current query.
		/// </summary>
		/// <param name="having">The having clause to apply.</param>
		/// <returns>The new Query object.</returns>
		public Query Having(string having) => new Query(this).Having(having);

		/// <summary>
		/// The order by clause to apply to the current query.
		/// </summary>
		/// <param name="orderBy">The order by clause to apply.</param>
		/// <returns>The new Query object.</returns>
		public Query OrderBy(string orderBy) => new Query(this).OrderBy(orderBy);

		/// <summary>
		/// Adds the specified columns to the list of columns to select for the current query.
		/// </summary>
		/// <param name="columns">The columns to select.</param>
		/// <returns>The new Query object.</returns>
		public Query Select(params string[] columns) => new Query(this).Select(columns);

		/// <summary>
		/// Executes the specified SQL command.
		/// 
		/// WARNING:    You can execute any SQL this way. Make sure to sanitize
		///             input and not delete everything/drop tables!
		/// </summary>
		/// <param name="sql">The SQL to execute.</param>
		/// <returns>The new Query object.</returns>
		public Query Sql(string sql) => new Query(this).Sql(sql);

		/// <summary>
		/// The name of the stored procedure to execute.
		/// </summary>
		/// <param name="storedProc">The stored procedure name.</param>
		/// <returns>The new Query object.</returns>
		public Query StoredProc(string storedProc) => new Query(this).StoredProc(storedProc);

		/// <summary>
		/// The where clause to apply to the current query.
		/// </summary>
		/// <param name="where">The where clause to apply.</param>
		/// <returns>The new Query object.</returns>
		public Query Where(string where) => new Query(this).Where(where);

		/// <summary>
		/// Add SQL parameters to the current query.
		/// </summary>
		/// <param name="parameters">The parameters to add to the current query.</param>
		/// <returns>The new Query object.</returns>
		public Query With(params SqlParameter[] parameters) => new Query(this).With(parameters);

		/// <summary>
		/// Add SQL parameters to the current query.
		/// </summary>
		/// <param name="parameters">The parameters to add to the current query as tuples.</param>
		/// <returns>The new Query object.</returns>
		public Query With(params (string Name, object Value)[] parameters) => new Query(this).With(parameters);

		#endregion

		public enum RequestType
		{
			None,
			Count,
			Delete,
			Execute,
			ID,
			Insert,
			InsertMultiple,
			Item,
			Items,
			ItemsYield,
			Page,
			Reader,
			ResultSets,
			SingleValue,
			Update
		}

		public class Query
		{
			private SqlHelper Database { get; set; }
			private string FilterVal { get; set; }
			private List<string> Columns { get; set; }
			private string FromVal { get; set; }
			private string WhereVal { get; set; }
			private string OrderByVal { get; set; }
			private string GroupByVal { get; set; }
			private string HavingVal { get; set; }
			private string StoredProcVal { get; set; }
			private List<SqlParameter> Parameters { get; set; }
			private string SqlVal { get; set; }

			// Helpers for specific query types.
			private Dictionary<string, object> UpdateValues { get; set; }
			private List<Dictionary<string, object>> InsertValueItems { get; set; }
			private string InsertValueItemsTvpType { get; set; }
			private int InsertValueItemsIndex { get; set; }
			private string OuterOrderVal { get; set; }
			private int PageNumber { get; set; }
			private int RowsPerPage { get; set; }

			private RequestType RequestType { get; set; }

			/* Priority Order:
			 * Sql
			 * StoredProc
			 * Built Query
			 */
			private CommandType CommandType => (string.IsNullOrEmpty(SqlVal) && !string.IsNullOrEmpty(StoredProcVal)) ? CommandType.StoredProcedure : CommandType.Text;

			public Query(SqlHelper database)
			{
				Database = database;
				Columns = new List<string>();
				Parameters = new List<SqlParameter>();
				InsertValueItemsIndex = 0;
			}

			private class SqlWithParameters
			{
				public string Sql { get; set; }
				public List<SqlParameter> Parameters { get; set; } = new List<SqlParameter>();
			}

			private SqlWithParameters CreateSqlQuery()
			{
				if (!string.IsNullOrEmpty(SqlVal))
				{
					return new SqlWithParameters { Sql = SqlVal };
				}
				else if (!string.IsNullOrEmpty(StoredProcVal))
				{
					return new SqlWithParameters { Sql = StoredProcVal };
				}
				switch (RequestType)
				{
					case RequestType.Delete:
						return CreateDeleteQuery();
					case RequestType.Insert:
					case RequestType.InsertMultiple:
						return InsertValueItemsTvpType == null ? CreateInsertQuery() : CreateInsertTvpQuery();
					case RequestType.Page:
						return CreatePageQuery();
					case RequestType.Update:
						return CreateUpdateQuery();
				}
				return CreateGeneralQuery();
			}

			#region Command Builders

			/// <summary>
			/// Adds a value between 'SELECT' and the list of fields, such as 'TOP 100' to the current query.
			/// </summary>
			/// <param name="filter">Any value between 'SELECT' and the list of fields, such as 'TOP 100'</param>
			/// <returns>The current Query object.</returns>
			public Query Filter(string filter)
			{
				FilterVal = filter;
				return this;
			}

			/// <summary>
			/// The View, Table or Join to select from.
			/// </summary>
			/// <param name="from">The view, table or join to select from.</param>
			/// <returns>The current Query object.</returns>
			public Query From(string from)
			{
				FromVal = from;
				return this;
			}

			/// <summary>
			/// The group by clause to apply to the current query.
			/// </summary>
			/// <param name="groupBy">The group by clause to apply.</param>
			/// <returns>The current Query object.</returns>
			public Query GroupBy(string groupBy)
			{
				GroupByVal = groupBy;
				return this;
			}

			/// <summary>
			/// The having clause to apply to the current query.
			/// </summary>
			/// <param name="having">The having clause to apply.</param>
			/// <returns>The current Query object.</returns>
			public Query Having(string having)
			{
				HavingVal = having;
				return this;
			}

			/// <summary>
			/// The order by clause to apply to the current query.
			/// </summary>
			/// <param name="orderBy">The order by clause to apply.</param>
			/// <returns>The current Query object.</returns>
			public Query OrderBy(string orderBy)
			{
				OrderByVal = orderBy;
				return this;
			}

			/// <summary>
			/// Adds the specified columns to the list of columns to select for the current query.
			/// </summary>
			/// <param name="columns">The columns to select.</param>
			/// <returns>The current Query object.</returns>
			public Query Select(params string[] columns)
			{
				Columns.AddRange(columns);
				return this;
			}

			/// <summary>
			/// Executes the specified SQL command.
			/// 
			/// WARNING:    You can execute any SQL this way. Make sure to sanitize
			///             input and not delete everything/drop tables!
			/// </summary>
			/// <param name="sql">The SQL to execute.</param>
			/// <returns>The current Query object.</returns>
			public Query Sql(string sql)
			{
				SqlVal = sql;
				return this;
			}

			/// <summary>
			/// The name of the stored procedure to execute.
			/// </summary>
			/// <param name="storedProc">The stored procedure name.</param>
			/// <returns>The current Query object.</returns>
			public Query StoredProc(string storedProc)
			{
				StoredProcVal = storedProc;
				return this;
			}

			/// <summary>
			/// The where clause to apply to the current query.
			/// </summary>
			/// <param name="where">The where clause to apply.</param>
			/// <returns>The current Query object.</returns>
			public Query Where(string where)
			{
				WhereVal = where;
				return this;
			}

			/// <summary>
			/// Add SQL parameters to the current query.
			/// </summary>
			/// <param name="parameters">The parameters to add to the current query.</param>
			/// <returns>The current Query object.</returns>
			public Query With(params SqlParameter[] parameters)
			{
				Parameters.AddRange(parameters);
				return this;
			}

			/// <summary>
			/// Add SQL parameters to the current query.
			/// </summary>
			/// <param name="parameters">The parameters to add to the current query as tuples.</param>
			/// <returns>The current Query object.</returns>
			public Query With(params (string Name, object Value)[] parameters)
			{
				foreach (var (name, value) in parameters)
					Parameters.Add(new SqlParameter(name, value));
				return this;
			}

			#endregion

			private void SetRequestTypeAndValidate(RequestType type)
			{
				// Type is set when a data retriever is called. Since data retrievers can call each other
				// to get their work done, we only take the first value and validate once.
				if (RequestType == RequestType.None)
				{
					RequestType = type;
					Validate();
				}
			}

			private void Validate()
			{
				bool sql = !string.IsNullOrEmpty(SqlVal);
				bool sp = !string.IsNullOrEmpty(StoredProcVal);
				bool from = !string.IsNullOrEmpty(FromVal);
				bool pieces = !string.IsNullOrEmpty(FilterVal) || Columns.Count > 0 || !string.IsNullOrEmpty(WhereVal) || !string.IsNullOrEmpty(OrderByVal) ||
					!string.IsNullOrEmpty(GroupByVal) || !string.IsNullOrEmpty(HavingVal);

				if (sql)
				{
					if (sp)
					{
						throw new SqlHelperException("Cannot have both direct SQL and a StoredProc clause.");
					}
					if (from)
					{
						throw new SqlHelperException("Cannot have both direct SQL and a From clause.");
					}
					if (pieces)
					{
						throw new SqlHelperException("Cannot have both direct SQL and other query clauses.");
					}
				}
				if (sp && from)
				{
					throw new SqlHelperException("Cannot have both StoredProc and From clauses.");
				}
				if (sp && pieces)
				{
					throw new SqlHelperException("Cannot have both StoredProc and other query clauses.");
				}

				switch (RequestType)
				{
					case RequestType.Execute:
					case RequestType.Items:
					case RequestType.ItemsYield:
					case RequestType.Reader:
					case RequestType.ResultSets:
					case RequestType.SingleValue:
						break;

					case RequestType.Count:
						if (Columns.Count > 0)
						{
							throw new SqlHelperException("Count cannot be called with a Select clause.");
						}
						ValidateEmpty(false, FromVal, "Count", "From");
						break;
					case RequestType.Delete:
						ValidateEmpty(true, FilterVal, "Delete", "Filter");
						if (Columns.Count > 0)
						{
							throw new SqlHelperException("Delete cannot be called with a Select clause.");
						}
						ValidateEmpty(false, FromVal, "Delete", "From");
						ValidateEmpty(false, WhereVal, "Delete", "Where");
						ValidateEmpty(true, OrderByVal, "Delete", "OrderBy");
						ValidateEmpty(true, GroupByVal, "Delete", "GroupBy");
						ValidateEmpty(true, HavingVal, "Delete", "Having");
						break;
					case RequestType.ID:
						ValidateEmpty(false, StoredProcVal, "ID", "StoredProc");
						break;
					case RequestType.Insert:
					case RequestType.InsertMultiple:
						ValidateEmpty(true, FilterVal, "Insert", "Filter");
						if (Columns.Count > 0)
						{
							throw new SqlHelperException("Insert cannot be called with a Select clause.");
						}
						ValidateEmpty(false, FromVal, "Insert", "From");
						ValidateEmpty(true, WhereVal, "Insert", "Where");
						ValidateEmpty(true, OrderByVal, "Insert", "OrderBy");
						ValidateEmpty(true, GroupByVal, "Insert", "GroupBy");
						ValidateEmpty(true, HavingVal, "Insert", "Having");
						if (InsertValueItems == null || InsertValueItems.Count == 0)
						{
							throw new SqlHelperException("Insert must be called with one or more values.");
						}
						break;
					case RequestType.Item:
						ValidateEmpty(true, FilterVal, "Item", "Filter");
						ValidateEmpty(false, FromVal, "Item", "From");
						break;
					case RequestType.Page:
						ValidateEmpty(false, FromVal, "Page", "From");
						ValidateEmpty(false, OrderByVal, "Page", "OrderBy");
						break;
					case RequestType.Update:
						ValidateEmpty(true, FilterVal, "Update", "Filter");
						if (Columns.Count > 0)
						{
							throw new SqlHelperException("Update cannot be called with a Select clause.");
						}
						ValidateEmpty(false, FromVal, "Update", "From");
						ValidateEmpty(false, WhereVal, "Update", "Where");
						ValidateEmpty(true, OrderByVal, "Update", "OrderBy");
						ValidateEmpty(true, GroupByVal, "Update", "GroupBy");
						ValidateEmpty(true, HavingVal, "Update", "Having");
						if (UpdateValues == null || UpdateValues.Count == 0)
						{
							throw new SqlHelperException("Update must be called with one or more values.");
						}
						break;
					default:
						throw new SqlHelperException("Internal error, unknown request type " + RequestType);
				}
			}

			private void ValidateEmpty(bool empty, string val, string type, string clause)
			{
				if (string.IsNullOrEmpty(val) != empty)
				{
					throw new SqlHelperException(type + (empty ? " cannot be called with a " : " must be called with a ") + clause + " clause.");
				}
			}

			#region Query Creators

			private SqlWithParameters CreateDeleteQuery()
			{
				var sb = new StringBuilder("DELETE FROM ");
				sb.Append(FromVal);
				sb.Append(" WHERE ");
				sb.Append(WhereVal);
				return new SqlWithParameters { Sql = sb.ToString() };
			}

			private SqlWithParameters CreateGeneralQuery()
			{
				var sb = new StringBuilder("SELECT ");
				if (!string.IsNullOrEmpty(FilterVal))
				{
					sb.Append(FilterVal);
					sb.Append(" ");
				}
				sb.Append(Columns.Count == 0 ? "*" : string.Join(", ", Columns));
				if (!string.IsNullOrEmpty(FromVal))
				{
					sb.Append(" FROM ");
					sb.Append(FromVal);
				}
				if (!string.IsNullOrEmpty(WhereVal))
				{
					sb.Append(" WHERE ");
					sb.Append(WhereVal);
				}
				if (!string.IsNullOrEmpty(GroupByVal))
				{
					sb.Append(" GROUP BY ");
					sb.Append(GroupByVal);
				}
				if (!string.IsNullOrEmpty(HavingVal))
				{
					sb.Append(" HAVING ");
					sb.Append(HavingVal);
				}
				if (!string.IsNullOrEmpty(OrderByVal))
				{
					sb.Append(" ORDER BY ");
					sb.Append(OrderByVal);
				}
				return new SqlWithParameters { Sql = PreventSniffing(sb.ToString()) };
			}

			private SqlWithParameters CreateInsertQuery()
			{
				var parameters = new Dictionary<object, int>();
				var sql = new SqlWithParameters();
				var sb = new StringBuilder();
				int counter = 0;

				var keys = new List<string>();
				keys.AddRange(InsertValueItems[0].Keys);
				sb.Append("INSERT INTO ");
				sb.Append(FromVal);
				sb.Append(" (");
				string sep = "";
				foreach (var k in keys)
				{
					sb.Append(sep);
					sep = ",";
					sb.Append("[");
					sb.Append(k);
					sb.Append("]");
				}
				sb.Append(") VALUES ");
				string outerSep = "";
				int rowCount = 0;
				// SQL Server claims to support 2100 parameters, but throws an exception with that count.
				// It appears to actually support 2099, hence the use of < instead of <=.
				for (int i = InsertValueItemsIndex; i < InsertValueItems.Count && ((sql.Parameters.Count + keys.Count) < MAX_PARAMETERS) && (rowCount < MAX_ROWS); i++)
				{
					sb.Append(outerSep);
					outerSep = ",";
					sb.Append("(");
					sep = "";
					foreach (var k in keys)
					{
						// Get or create parameter.
						object val = InsertValueItems[i][k] ?? DBNull.Value;
						int paramIndex = counter;
						if (parameters.ContainsKey(val))
						{
							paramIndex = parameters[val];
						}
						else
						{
							parameters[val] = paramIndex;
							sql.Parameters.Add(new SqlParameter(paramIndex.ToString(), val));
							counter++;
						}

						sb.Append(sep);
						sep = ",";
						sb.Append("@");
						sb.Append(paramIndex);
					}
					sb.Append(")");

					InsertValueItemsIndex++;
					rowCount++;
				}
				if (RequestType == RequestType.Insert)
				{
					sb.Append(";SELECT SCOPE_IDENTITY();");
				}

				sql.Sql = sb.ToString();
				return sql;
			}

			private SqlWithParameters CreateInsertTvpQuery()
			{
				var sql = new SqlWithParameters();
				var sb = new StringBuilder("INSERT INTO ");
				sb.Append(FromVal);
				sb.Append(" (");

				var keys = new List<string>();
				keys.AddRange(InsertValueItems[0].Keys);
				string sep = "";
				var colsSb = new StringBuilder();
				foreach (var k in keys)
				{
					colsSb.Append(sep);
					sep = ",";
					colsSb.Append("[");
					colsSb.Append(k);
					colsSb.Append("]");
				}

				sb.Append(colsSb);
				sb.Append(") SELECT ");
				sb.Append(colsSb);
				sb.Append(" FROM @tvp");
				sql.Sql = sb.ToString();
				sql.Parameters.Add(TableValuedParameter("tvp", InsertValueItems, InsertValueItemsTvpType));
				return sql;
			}

			private SqlWithParameters CreatePageQuery()
			{
				var sb = new StringBuilder("SELECT * FROM ( SELECT ");
				if (!string.IsNullOrEmpty(FilterVal))
				{
					sb.Append(FilterVal);
					sb.Append(" ");
				}
				sb.Append(Columns.Count == 0 ? "*" : string.Join(", ", Columns));
				sb.Append(", ROW_NUMBER() OVER(ORDER BY ");
				sb.Append(OrderByVal);
				sb.Append(") AS _RowNumber FROM ");
				sb.Append(FromVal);
				if (!string.IsNullOrEmpty(WhereVal))
				{
					sb.Append(" WHERE ");
					sb.Append(WhereVal);
				}
				if (!string.IsNullOrEmpty(GroupByVal))
				{
					sb.Append(" GROUP BY ");
					sb.Append(GroupByVal);
				}
				if (!string.IsNullOrEmpty(HavingVal))
				{
					sb.Append(" HAVING ");
					sb.Append(HavingVal);
				}
				sb.Append(") AS Tbl WHERE _RowNumber BETWEEN ");
				sb.Append(PageNumber * RowsPerPage + 1);
				sb.Append(" AND ");
				sb.Append((PageNumber + 1) * RowsPerPage);
				if (!string.IsNullOrEmpty(OuterOrderVal))
				{
					sb.Append(" ORDER BY ");
					sb.Append(OuterOrderVal);
				}
				return new SqlWithParameters { Sql = PreventSniffing(sb.ToString()) };
			}

			private SqlWithParameters CreateUpdateQuery()
			{
				var sql = new SqlWithParameters();
				var sb = new StringBuilder("UPDATE ");
				sb.Append(FromVal);
				sb.Append(" SET ");
				string sep = "";
				foreach (var v in UpdateValues)
				{
					sb.Append(sep);
					sep = ", ";
					sb.Append(v.Key);
					sb.Append("=@");
					sb.Append(v.Key);
					sql.Parameters.Add(new SqlParameter(v.Key, v.Value));
				}
				sb.Append(" WHERE ");
				sb.Append(WhereVal);
				sql.Sql = sb.ToString();
				return sql;
			}

			#endregion

			#region Data Retrievers

			/// <summary>
			/// Adds COUNT(*) to the items to select and executes the query.
			/// </summary>
			/// <returns>The results of selecting COUNT(*).</returns>
			public decimal Count()
			{
				SetRequestTypeAndValidate(RequestType.Count);
				Columns.Add("COUNT(*)");
				return SingleValue<decimal>();
			}

			/// <summary>
			/// Deletes the current query results.
			/// </summary>
			/// <returns>The number of rows affected.</returns>
			public int Delete()
			{
				SetRequestTypeAndValidate(RequestType.Delete);
				return Execute();
			}

			/// <summary>
			/// Executes the current query.
			/// </summary>
			/// <returns>The number of rows affected.</returns>
			public int Execute()
			{
				SetRequestTypeAndValidate(RequestType.Execute);
				using (var cmd = BuildSqlCommand())
				{
					return cmd.ExecuteNonQuery();
				}
			}

			/// <summary>
			/// Adds an ID out parameter and executes the query.
			/// </summary>
			/// <returns>The value of the ID parameter.</returns>
			public int ID()
			{
				SetRequestTypeAndValidate(RequestType.ID);
				using (var cmd = BuildSqlCommand())
				{
					var id = new SqlParameter("ID", SqlDbType.Int) { Direction = ParameterDirection.Output };
					cmd.Parameters.Add(id);
					cmd.ExecuteNonQuery();
					return (int)id.Value;
				}
			}

			/// <summary>
			/// Inserts the specified values into the current query table.
			/// </summary>
			/// <param name="values">Dictionary of columns and values to insert.</param>
			/// <returns>ID of the item inserted, or zero if no identity exists.</returns>
			public decimal Insert(Dictionary<string, object> values)
			{
				InsertValueItems = new List<Dictionary<string, object>>();
				InsertValueItems.Add(values);
				SetRequestTypeAndValidate(RequestType.Insert);
				return SingleValue<decimal?>() ?? 0;
			}

			/// <summary>
			/// Inserts the specified items with the specified values into the current query table.
			/// </summary>
			/// <param name="valueItems">The list of dictionaries of columns and values to insert.</param>
			/// <returns>The number of items inserted.</returns>
			public int Insert(List<Dictionary<string, object>> valueItems)
			{
				InsertValueItems = valueItems;
				SetRequestTypeAndValidate(RequestType.InsertMultiple);
				int sum = 0;
				while (InsertValueItemsIndex < InsertValueItems.Count)
				{
					sum += Execute();
				}
				return sum;
			}

			/// <summary>
			/// Inserts the specified items with the specified values into the current query table.
			/// </summary>
			/// <param name="valueItems">The list of dictionaries of columns and values to insert.</param>
			/// <param name="type">The table-valued parameter's type name.</param>
			/// <returns>The number of items inserted.</returns>
			public int Insert(List<Dictionary<string, object>> valueItems, string type)
			{
				InsertValueItems = valueItems;
				InsertValueItemsTvpType = type;
				SetRequestTypeAndValidate(RequestType.InsertMultiple);
				return Execute();
			}

			/// <summary>
			/// Executes the current query, returning the first row or null.
			/// </summary>
			/// <returns>The first row of results, or null.</returns>
			public Dictionary<string, object> Item()
			{
				SetRequestTypeAndValidate(RequestType.Item);
				FilterVal = "TOP 1";
				Dictionary<string, object> item = null;
				using (var cmd = BuildSqlCommand())
				{
					var reader = cmd.ExecuteReader();
					if (reader.Read())
					{
						var cols = new List<string>();
						EnsureColumnNames(cols, reader);
						item = new Dictionary<string, object>();
						for (int i = 0; i < cols.Count; i++)
						{
							item.Add(cols[i], reader.IsDBNull(i) ? null : reader.GetValue(i));
						}
					}
					reader.Close();
				}
				return item;
			}

			/// <summary>
			/// Executes the current query, retrieving the specified data, and optionally calling a
			/// function on each item returned.
			/// </summary>
			/// <param name="itemAction">A function to call on every item returned.</param>
			/// <returns>A list of Dictionary&lt;string, object&gt; objects that contain the requested data.</returns>
			public List<Dictionary<string, object>> Items(Action<Dictionary<string, object>> itemAction = null)
			{
				SetRequestTypeAndValidate(RequestType.Items);
				var results = new List<Dictionary<string, object>>();
				using (var cmd = BuildSqlCommand())
				{
					var cols = new List<string>();
					var reader = cmd.ExecuteReader();
					while (reader.Read())
					{
						EnsureColumnNames(cols, reader);

						var item = new Dictionary<string, object>();
						results.Add(item);

						for (int i = 0; i < cols.Count; i++)
						{
							item.Add(cols[i], reader.IsDBNull(i) ? null : reader.GetValue(i));
						}
					}
					reader.Close();
				}
				if (itemAction != null)
				{
					foreach (var item in results) { itemAction(item); }
				}
				return results;
			}

			/// <summary>
			/// Executes the current query, retrieving the specified data. 
			/// 
			/// This version uses a yield to return large data sets more efficiently, but you cannot run another query
			/// until this one is complete.
			/// </summary>
			/// <returns>A list of Dictionary&lt;string, object&gt; objects that contain the requested data.</returns>
			public IEnumerable<Dictionary<string, object>> ItemsYield()
			{
				SetRequestTypeAndValidate(RequestType.ItemsYield);
				using (var cmd = BuildSqlCommand())
				{
					var cols = new List<string>();
					var reader = cmd.ExecuteReader();
					while (reader.Read())
					{
						EnsureColumnNames(cols, reader);
						var item = new Dictionary<string, object>();
						for (int i = 0; i < cols.Count; i++)
						{
							item.Add(cols[i], reader.IsDBNull(i) ? null : reader.GetValue(i));
						}
						yield return item;
					}
					reader.Close();
				}
			}

			/// <summary>
			/// Retrieves a list of Dictionary&lt;string, object&gt; objects that contain the requested columns from the
			/// specified source.
			/// </summary>
			/// <param name="outerOrder">The outer select order clause since parameters may not be named the same
			/// as the inner select that contains any JOINs, etc.</param>
			/// <param name="pageNumber">Zero-based page number.</param>
			/// <param name="rowsPerPage">Rows to retrieve per page.</param>
			/// <returns>The list of items the query returned.</returns>
			public List<Dictionary<string, object>> Page(string outerOrder, int pageNumber, int rowsPerPage)
			{
				if (rowsPerPage > 0)
				{
					SetRequestTypeAndValidate(RequestType.Page);
					OuterOrderVal = outerOrder;
					PageNumber = pageNumber;
					RowsPerPage = rowsPerPage;
				}

				return Items();
			}

			/// <summary>
			/// Executes the current query, returning a SqlDataReader to the data.
			/// </summary>
			/// <returns>A SqlDataReader containing the requested data.</returns>
			public SqlDataReader Reader()
			{
				SetRequestTypeAndValidate(RequestType.Reader);
				using (var cmd = BuildSqlCommand())
				{
					return cmd.ExecuteReader();
				}
			}

			/// <summary>
			/// Retrieves a list of lists of Dictionary&lt;string, object&gt; objects that contains the requested data.
			/// Allows you to access the data from multiple result sets.
			/// </summary>
			/// <returns>The result sets from the current query.</returns>
			public List<List<Dictionary<string, object>>> ResultSets()
			{
				SetRequestTypeAndValidate(RequestType.ResultSets);
				var results = new List<List<Dictionary<string, object>>>();
				using (var cmd = BuildSqlCommand())
				{
					var reader = cmd.ExecuteReader();
					do
					{
						var cols = new List<string>();

						var resultSet = new List<Dictionary<string, object>>();
						results.Add(resultSet);

						while (reader.Read())
						{
							EnsureColumnNames(cols, reader);

							var item = new Dictionary<string, object>();
							resultSet.Add(item);
							for (int i = 0; i < cols.Count; i++)
							{
								item.Add(cols[i], reader.IsDBNull(i) ? null : reader.GetValue(i));
							}
						}
					} while (reader.NextResult());

					reader.Close();
				}
				return results;
			}

			/// <summary>
			/// Gets the first value from the current query, cast as the specified type.
			/// </summary>
			/// <typeparam name="T">The type of the value.</typeparam>
			/// <returns>The first value from the current query.</returns>
			public T SingleValue<T>()
			{
				SetRequestTypeAndValidate(RequestType.SingleValue);
				using (var cmd = BuildSqlCommand())
				{
					var type = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
					var val = cmd.ExecuteScalar();
					if (val == null || val.GetType() == typeof(DBNull))
					{
						return (T)default;
					}

					return (T)Convert.ChangeType(val, type);
				}
			}

			/// <summary>
			/// Updates the specified values on the current query object.
			/// </summary>
			/// <param name="updatedValues">Dictionary of columns and values to update.</param>
			/// <returns>The number of rows affected.</returns>
			public int Update(Dictionary<string, object> updatedValues)
			{
				UpdateValues = updatedValues;
				SetRequestTypeAndValidate(RequestType.Update);
				return Execute();
			}

			#endregion

			#region Private Helper Methods

			private SqlCommand BuildSqlCommand()
			{
				var sql = CreateSqlQuery();
				var cmd = (Database.Transaction == null ? new SqlCommand(sql.Sql, Database.Connection) : new SqlCommand(sql.Sql, Database.Connection, Database.Transaction));
				cmd.CommandTimeout = Database.CommandTimeout;
				cmd.CommandType = CommandType;
				foreach (var p in Parameters)
				{
					if (p.Value == null) { p.Value = DBNull.Value; }
					cmd.Parameters.Add(p);
				}
				foreach (var p in sql.Parameters)
				{
					if (p.Value == null) { p.Value = DBNull.Value; }
					cmd.Parameters.Add(p);
				}
				return cmd;
			}

			private static void EnsureColumnNames(List<string> columnNames, SqlDataReader reader)
			{
				if (columnNames.Count == 0)
				{
					for (int i = 0; i < reader.FieldCount; i++)
					{
						string name = reader.GetName(i);
						string nameBase = name;
						int count = 0;
						while (name.Length == 0 || columnNames.Contains(name))
						{
							name = nameBase + "_" + (count++);
						}
						columnNames.Add(name);
					}
				}
			}

			private string PreventSniffing(string sql)
			{
				string prefix = "_";
				var sb = new StringBuilder();
				foreach (var p in Parameters)
				{
					string name = p.ParameterName.TrimStart('@');
					string type = p.SqlDbType.ToString();
					if (p.Size > 0) { type += "(" + p.Size + ")"; }

					sb.Append("declare @");
					sb.Append(prefix);
					sb.Append(name);
					sb.Append(" ");
					sb.Append(type);
					sb.Append("=@");
					sb.AppendLine(name);
				}
				sb.Append(sql.Replace("@", "@" + prefix));
				return sb.ToString();
			}

			#endregion
		}
	}

	public class SqlHelperException : Exception
	{
		public SqlHelperException(string message) : base(message) { }
	}
}
