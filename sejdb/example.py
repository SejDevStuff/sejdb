import time
import random
import sejdb

TABLE_TEST_1 = random.randint(0,9999)
TABLE_TEST_2 = random.randint(0,9999)
TABLE_TEST_3 = random.randint(0,9999)

def TableCreate():
    print("\nRunning benchmark: CREATE 10k TABLES")

    start_create_tables = time.time()

    for i in range(10000):
        db.CreateTable(str(i), 3, ["A", "B", "C"])

    end_create_tables = time.time()

    print("Created 10,000 tables in " + str(end_create_tables - start_create_tables) + " seconds")

def RowAdd():
    print("\nRunning benchmark: ADD 10k ROWS RANDOMLY (InsertRowIntoTable)")

    start_row_add = time.time()

    db.InsertRowIntoTable(str(TABLE_TEST_1), ["1", "2", "3"])
    db.InsertRowIntoTable(str(TABLE_TEST_2), ["1", "2", "3"])
    db.InsertRowIntoTable(str(TABLE_TEST_3), ["1", "2", "3"])

    for i in range(9997):
        db.InsertRowIntoTable(str(random.randint(0,9999)), ["1", "2", "3"])

        if (db.IsError()):
            print(db.GetError())
            return

    end_row_add = time.time()
    print("Added 10,000 rows randomly in " + str(end_row_add - start_row_add) + " seconds")

def RowAddOneTbl():
    print("\nRunning benchmark: ADD 10k ROWS TO ONE TABLE (InsertMultipleRowsIntoTable)")

    # generate data
    payload = []
    for i in range(10000):
        payload.append(["4", "5", "6"])

    start_row_add = time.time()

    db.InsertMultipleRowsIntoTable("5000", payload)

    end_row_add = time.time()
    print("Added 10,000 rows to one table in " + str(end_row_add - start_row_add) + " seconds")

def Search():
    print("\nRunning benchmark: SEARCH FOR 5 ROWS (GetRows)")

    start_search = time.time()

    rowTest1 = db.GetRows("5000", "C", ">=", "4", 1)                # should return 1 row
    rowTest2 = db.GetRows(str(TABLE_TEST_1), "A", "==", "5", 1)     # should return 0 rows
    rowTest3 = db.GetRows(str(TABLE_TEST_2), "B", ">=", "0", 1)     # should return 1 row
    rowTest4 = db.GetRows(str(TABLE_TEST_3), "C", "!=", "2", 1)     # should return 1 row
    rowTest5 = db.GetRows(str(TABLE_TEST_2), "A", "<", "5", 1)      # should return 1 row
    
    end_search = time.time()

    if ( len(rowTest1) + len(rowTest2) + len(rowTest3) + len(rowTest4) + len(rowTest5)  ) != 4:
        print("Error! Invalid results")
        return
    
    if ( len(rowTest2) != 0 ):
        print("Error! Invalid results")
        return

    print("Searched for 5 random rows in " + str(end_search - start_search) + " seconds")

def Del():
    print("\nRunning benchmark: DELETE 10k ROWS (DelRow)")

    rows = db.GetRows("5000", "A", "==", "4", 10000)

    if (len(rows) != 10000):
        print("Error 1! Cannot find rows")
        return
    
    start_del = time.time()
    db.DelRows("5000", rows)
    end_del = time.time()

    rows = db.GetRows("5000", "A", "==", "4")

    if (len(rows) != 0):
        print("Error 2! Rows not deleted")
        return
    
    print("Deleted 10k rows in " + str(end_del - start_del) + " seconds")


sejdb.CreateSejDB("./example.sdb")
db = sejdb.SejDBInstance("./example.sdb", 1)

TableCreate()
RowAdd()
RowAddOneTbl()
Search()
Del()

if (db.IsError()):
   print(db.GetError())

db.CloseDB()