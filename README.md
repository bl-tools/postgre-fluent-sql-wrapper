postgre-fluent-sql-wrapper
===

Library provides the ability to call Stored Procedures, Functions, Raw SQL in PostgreDB. It is a wrapper over Npgsql data provider. Library created for the ability to use data provider in fluent notation mode.

```csharp
//call function with parameters
var command = new FluentSqlCommand("connection string");
var outgoingData = await command.Function("functionName")
                                .AddParam("param1", "value1")
                                .ExecReadAsync((reader)=>
                                      {
                                          var result = new OutgoingDataModel
                                          {
                                              field1 = reader.GetString("field1"),
                                              field2 = reader.GetString("field2")
                                          };
                                          return result;
                                        });
```

```csharp
//call stored procedure with returning data
var command = new FluentSqlCommand("connection string");
var outgoingData = command.StoredProcedure("CALL storedProcedureName(@param1, @param2, 'refcursor');", "refcursor")
                          .AddParam("@param1", "value1")
                          .AddParam("@param2", "value2")
                          .ExecRead((reader)=>
                               {
                                   var result = new OutgoingDataModel
                                   {
                                       field1 = reader.GetString("field1"),
                                       field2 = reader.GetString("field2"),
                                       field3 = reader.GetInt32("field3"),
                                       field4 = reader.GetInt32("field4"),
                                       field5 = reader.GetDateTime("field5")
                                   };
                                   return result;
                               });
```


An example of calling stored procedure which returns multiple data sets. Note that the data sets can have different columns:

```csharp
public Results LoadResults(int batchSize)
{
	var result = new Results
	{
		FirstResultSet = new List<FirstResultSetType>(),
		SecondResultSet = new List<SecondResultSetType>()
	};

	new FluentSqlCommand("connection string")
		.StoredProcedure("CALL someStoredProc(@batchSize, 'p_refcur1', 'p_refcur2')", 
			new List<string>(){ "p_refcur1", "p_refcur2" })
		.AddParam("batchSize", batchSize)
		.ExecReadList((reader, resultSet) => FillRecords(result, reader, resultSet));

	return result;
}


private static void FillRecords(Results result, IDataRecord record, int resultNumber)
{
	switch (resultNumber)
	{
		case 0:
			result.FirstResultSet.Add(ReadFirstResultSetRecord(record));
			break;
		case 1:
			result.SecondResultSet.Add(ReadSecondResultSetRecord(record));
			break;
		default:
			return;
	}
}

private static FirstResultSetType ReadFirstResultSetRecord(IDataRecord record)
{
	return new FirstResultSetType
	{
		RecordId = record.GetGuid("record_id"),
		Value1 = record.GetInt32("value_1"),
		Value2 = record.GetString("value_2")
	};
}

private static SecondResultSetType ReadSecondResultSetRecord(IDataRecord record)
{
	return new SecondResultSetType
	{
		RecordId = record.GetGuid("record_id"),
		Value3 = record.GetString("value_3"),
		Value4 = record.GetString("value_4")
	};
}
```
