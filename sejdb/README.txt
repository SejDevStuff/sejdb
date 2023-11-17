* CreateSejDB(dbPath)
Creates a new SejDB file at the designated path. Prints any errors to stdout.
Returns: 0 on success, 1 on fail

--- The following functions are part of the SejDBInstance class ---

* __init__(dbpath, accessmode)
This is the constructor for the class. dbpath should be a valid path to an existing SejDB file. accessmode is how you want to access the db, 0 means read only and 1 means read/write.

Returns: nothing

* IsError()
This will show if the error_message variable has been set, indicating that something went wrong when processing a query.

Returns: True if error, False if not error

* GetError()
Returns the contents of error_message. This will be set to hold some error information if an error did occur, otherwise it would be an empty string.

Returns: string

* CloseDB()
This unlocks the database (only if another process isn't using it) and releases the file handle, allowing other instances to use it.

Returns: True if successful, False if not.

* CreateTable(name, entries, entryNames)
This creates a table in the database. "name" is the name of the table, "entries" is an integer, representing the number of entries (columns) the table has (from 1 - 65535), and "entryNames" is an array containing the names of all the entries. The length of "entryNames" must be equal to "entries".

Returns: True if successful, False if not.

* GetTableInfo(tableName)
Gets information about a table in the database.

Returns:    a tuple with: table name (str), no. of entries in table (int), the location where the table header ends and table data begins (int), the names of the table entries (array of str)
            IF THE TABLE DOES NOT EXIST, then it returns "None" for all four values.

* InsertMultipleRowsIntoTable(tableName, multipleRowsData)
Inserts multiple rows into table. Faster than calling "InsertRowIntoTable" multiple times for the same table.
tableName is the name of the table to insert rows into, multipleRowsData is an array of rows to insert. Each row itself is an array which is the same size as the number of entries in the table, each element of the row array corresponds with that number entry in the table.

Returns: True if successful, False if not.

* InsertRowIntoTable(tableName, rowData)
Inserts a single row into a table. If you are adding multiple rows into the same table, consider using InsertMultipleRowsIntoTable, as it is much faster.
tableName is the name of the table to insert rows into, rowData is an array which is the same size as the number of entries in the table, each element of the array corresponds with that number entry in the table.

Returns: True if successful, False if not.

* DelRows(tableName, rowList)
Deletes rows from tables. tableName is the name of the table to delete rows from. rowList is the list of rows, provided by the GetRows() function.

Returns:    True if it has deleted every single row in rowList.
            False without an error flag set (i.e.: IsError() is False) if at least 1 row from rowList had been deleted, but not all rows could be deleted.
            False with an error flag set if no rows could be deleted, or if there is another error.

* GetRows(tableName, entryName, comparisonOperator, data, limit)
Gets a certain amount of rows from a table. The amount is chosen by the value of "limit" (an integer).
tableName is the table to get rows from, entryName is the entry in each row to compare, comparisonOperator determines how to compare an entry ("==", "!=", "<", ">", "<=", or ">="), data is the data to compare the entry at entryName of each row with by using the comparisonOperator.

Returns: a list, either empty or with rows, represented as dicts - with key being the entryName and the value being the row value, in it.
In either case, an error may have occurred, and the error flag may have been set. Even if you receive a list with data, check if the error flag has been set.
If the list is empty, and the error flag is unset, then there simply may not be any rows that match the criteria.