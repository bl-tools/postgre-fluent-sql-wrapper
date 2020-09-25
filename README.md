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
