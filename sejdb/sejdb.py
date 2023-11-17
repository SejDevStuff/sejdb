import io
import os
import numpy as np
from operator import itemgetter

def CreateSejDB(dbPath):
    if os.path.exists(dbPath):
        print("CreateSejDB: The DB already exists")
        return 1
    try:
        f = open(dbPath, 'w')
        f.write("<S2>0")
        f.close()

        return 0
    except:
        print("CreateSejDB: Error whilst writing to DB")
        return 1

class SejDBInstance:
    def __init__(self, dbpath: str, accessmode: int):
        self.path = dbpath
        self.access = accessmode

        self.error_message = ""

        f = None

        if (self.access == 0 or self.access == 1):
            try:
                f = open(self.path, "r+b")
            except:
                self.__setError("Cannot open DB")
                return
        else:
            self.__setError("Unknown access mode")
            return
        
        if f == None:
            self.__setError("Unknown error whilst opening DB")
            return

        magicBytes = f.read(4)

        if magicBytes != b'<S2>':
            if magicBytes != b'<SK>':
                self.__setError("Older SEJDB file")
            else:
                self.__setError("Not a SEJDB file")
            return

        lock = f.read(1)

        if lock == b'1':
            self.__setError("Database is being used by another process")
            return

        f.seek(4)
        f.write(b'1')
        
        self.handle = f

        # get table list
        self.__getTables()

        self.__insert_data = {
            "last": "",
            "tbName": "", 
            "tbEntries": "",
            "tbNewRowByte": ""
        }
    
    def __setError(self, errorMessage: str):
        self.error_message = errorMessage

    def IsError(self):
        if self.error_message != "":
            return True
        else:
            return False
    
    def GetError(self):
        return self.error_message

    def CloseDB(self):
        if (self.IsError()):
            if (self.GetError() == "Database is being used by another process"):
                return False

        try:
            self.handle.seek(4)
            self.handle.write(b'0')
            self.handle.close()
            return True
        except:
            self.__setError("Could not close DB")
            return False

    def __getTables(self):
        if (self.IsError()):
            return

        self.handle.seek(5)
        self.TableList = []

        while (byte := self.handle.read(1)):
            if byte != b'\xff':
                continue
                        
            tb_name_len = int(np.frombuffer(self.handle.read(1), dtype=np.uint8))
            tb_name = self.handle.read(tb_name_len).decode()
            self.TableList.append(tb_name)

        return

    def CreateTable(self, name: str, entries: int, entryNames: str = []):
        if (self.IsError()):
            return False

        if self.access != 1:
            self.__setError("Bad access mode")
            return False
        
        if name in self.TableList:
            self.__setError("Table with same name already exists")
            return False
        
        if len(name) > 127 or len(name) < 1:
            self.__setError("Size of table name must be between 1-127 characters")
            return False
        
        if entries > 65535 or entries < 1:
            self.__setError("Unsupported number of entries (must be 1-65535)")
            return False
        
        if len(entryNames) != entries:
            self.__setError("Mismatched amount of entry names and entries")
            return False

        for entry in entryNames:
            if len(entry) > 127 or len(entry) < 1:
                self.__setError("Size of entry name must be between 1-127 characters")
                return False
        
        self.handle.seek(0, io.SEEK_END)

        # start of table
        self.handle.write(b'\xFF')

        # table name
        self.handle.write(np.uint8(len(name)))
        self.handle.write(str.encode(name))

        # entries (number)
        self.handle.write(np.uint16(entries))

        # entry names
        for entry in entryNames:
            self.handle.write(np.uint8(len(entry)))
            self.handle.write(str.encode(entry))
        
        self.TableList.append(name)
        self.handle.flush()

        return True

    def GetTableInfo(self, tableName):
        if (self.IsError()):
            return None, None, None, None

        if tableName not in self.TableList:
            self.__setError("Table does not exist")
            return None, None, None, None
        
        self.handle.seek(5)

        while (byte := self.handle.read(1)):
            if byte != b'\xff':
                continue

            tb_name_len = int(np.frombuffer(self.handle.read(1), dtype=np.uint8))
            tb_name = self.handle.read(tb_name_len).decode()

            if (tb_name != tableName):
                continue

            tb_entry_amount = int(np.frombuffer(self.handle.read(2), dtype=np.uint16))
            tb_entry_names = []

            tb_hdr_end_byte = self.handle.tell()
            for i in range(tb_entry_amount):
                tb_hdr_end_byte += 1
                tb_entry_name_sz = int(np.frombuffer(self.handle.read(1), dtype=np.uint8))
                tb_hdr_end_byte += tb_entry_name_sz
                tb_entry_names.append(self.handle.read(tb_entry_name_sz).decode())

            return tb_name, tb_entry_amount, tb_hdr_end_byte, tb_entry_names

        self.__setError("Could not find table")
        return None, None, None, None

    def InsertMultipleRowsIntoTable(self, tableName: str, multipleRowsData: list):
        if (self.IsError()):
            return False
        
        if self.access != 1:
            self.__setError("Bad access mode")
            return False

        if (len(multipleRowsData) == 1):
            return self.InsertRowIntoTable(tableName, multipleRowsData[0])

        if tableName not in self.TableList:
            self.__setError("Table doesn't exist")
            return False
        
        tbName, tbEntries, tbNewRowByte, tbEntryNames = self.GetTableInfo(tableName)
        
        if self.IsError():
            return False
        
        if tbName != tableName:
            self.__setError("There was an error fetching the table info")
            return False
        
        for rowData in multipleRowsData:
            if (tbEntries != len(rowData)):
                self.__setError("Number of entries provided and number of entries in table do not match")
                return False

            rowSize = tbEntries
            for data in rowData:
                rowSize += len(data)

            if (rowSize > 65535):
                self.__setError(f"The size of the row (+ {tbEntries} control bytes) exceeds the maximum allowable size of 65,535 bytes")
                return False

        # start writing superfast!
        for rowData in multipleRowsData:
            rowSize = tbEntries
            for data in rowData:
                rowSize += len(data)

            self.handle.seek(tbNewRowByte)
            after_entry = self.handle.read()
            self.handle.seek(tbNewRowByte)

            self.handle.write(np.uint16(rowSize))

            for data in rowData:
                self.handle.write(str.encode(data))
                self.handle.write(b'\xFE')
            
            self.handle.write(after_entry)
        self.handle.flush()

        return True
                    
    def InsertRowIntoTable(self, tableName: str, rowData: str = []):
        if (self.IsError()):
            return False

        if self.access != 1:
            self.__setError("Bad access mode")
            return False

        if tableName not in self.TableList:
            self.__setError("Table doesn't exist")
            return False
        
        tbName = self.__insert_data["tbName"]
        tbEntries = self.__insert_data["tbEntries"]
        tbNewRowByte = self.__insert_data["tbNewRowByte"]

        if (self.__insert_data["last"] != tableName):
            tbName, tbEntries, tbNewRowByte, tbEntryNames = self.GetTableInfo(tableName)
            self.__insert_data["tbName"] = tbName
            self.__insert_data["tbEntries"] = tbEntries
            self.__insert_data["tbNewRowByte"] = tbNewRowByte
            self.__insert_data["last"] = tableName
        
        if self.IsError():
            return False
        
        if tbName != tableName:
            self.__setError("There was an error fetching the table info")
            return False

        if (tbEntries != len(rowData)):
            self.__setError("Number of entries provided in parameter 'rowData' and number of entries in table do not match")
            return False

        rowSize = tbEntries
        for data in rowData:
            rowSize += len(data)

        if (rowSize > 65535):
            self.__setError(f"The size of the row (+ {tbEntries} control bytes) exceeds the maximum allowable size of 65,535 bytes")
            return False

        self.handle.seek(tbNewRowByte)
        after_entry = self.handle.read()

        # insert row
        self.handle.seek(tbNewRowByte)

        self.handle.write(np.uint16(rowSize))

        for data in rowData:
            self.handle.write(str.encode(data))
            self.handle.write(b'\xFE')
        
        self.handle.write(after_entry)
        self.handle.flush()

        return True
    
    def DelRows(self, tableName: str, rowList: list):
        tmpRowlist = rowList

        if self.IsError():
            return False
        
        if self.access != 1:
            self.__setError("Bad access mode")
            return False
        
        if tableName not in self.TableList:
            self.__setError("Table doesn't exist")
            return False
        
        if len(rowList) < 1:
            self.__setError("Parameter 'rowList' does not contain any entries")
            return False
        
        if type(rowList[0]) is not dict:
            self.__setError("Invalid data for parameter 'rowList'")
            return False
        
        tbName, tbEntries, tbNewRowByte, tbEntryNames = self.GetTableInfo(tableName)

        if self.IsError():
            return False
        
        if tbName != tableName:
            self.__setError("There was an error fetching the table info")
            return False
        
        for entryName in tbEntryNames:
            if (entryName not in list(rowList[0].keys())):
                self.__setError("An entry with the name given does not exist in the table")
                return False
        
        # I think we've checked everything. It is now safe to delete, I think.

        to_delete_addr = []

        self.handle.seek(0, 2)
        file_size = self.handle.tell()
        self.handle.seek(tbNewRowByte)

        while (self.handle.tell() <= file_size):
            # read next byte (nb) without advancing current position
            cur_pos = self.handle.tell()
            nb = self.handle.read(1)

            if (nb == b'\xff'):
                break

            self.handle.seek(cur_pos)

            row_start_byte = self.handle.tell()
            row_sz = int(np.frombuffer(self.handle.read(2), dtype=np.uint16))
            row_data = self.handle.read(row_sz)
            row_end_byte = self.handle.tell()

            row_arr = row_data.split(b'\xfe')[:tbEntries]
            row_arr = [x.decode() for x in row_arr]

            iterator = 0
            endPoint = len(tmpRowlist)

            while iterator < endPoint:
                row = tmpRowlist.pop()
                if list(row.values()) == row_arr:
                    to_delete_addr.append((row_start_byte, row_end_byte))
                    break
                tmpRowlist.insert(0, row)
                iterator += 1
        
        to_delete_addr = sorted(to_delete_addr, key=itemgetter(0))
        bytes_deleted = 0

        for start, end in to_delete_addr:
            start = start - bytes_deleted
            end = end - bytes_deleted

            self.handle.seek(end)
            after_data = self.handle.read()
            self.handle.seek(start)
            self.handle.write(after_data)

            bytes_deleted += (end - start)
        
        if (len(tmpRowlist) != 0):
            if (len(tmpRowlist) == len(rowList)):
                self.__setError("Could not delete any row provided")
            return False

        return True

    # Comparison operators: ==, !=, <, >, <=, >=
    def GetRows(self, tableName: str, entryName: str, comparisonOperator: str, data: str, limit = 1):
        if self.IsError():
            return []
        
        validCompOps = ["==","!=", "<", ">", "<=", ">="]
        if comparisonOperator not in validCompOps:
            self.__setError("Invalid comparison operator")
            return []
        
        if (tableName not in self.TableList):
            self.__setError("Table doesn't exist")
            return []
        
        tbName, tbEntries, tbNewRowByte, tbEntryNames = self.GetTableInfo(tableName)
        
        if self.IsError():
            return []
        
        if tbName != tableName:
            self.__setError("There was an error fetching the table info")
            return []

        if entryName not in tbEntryNames:
            self.__setError("An entry with the name given does not exist in the table")
            return []
        
        checkEntry = tbEntryNames.index(entryName)
        found = []

        # iterate until next table, or until end of file
        self.handle.seek(0, 2)
        file_size = self.handle.tell()
        self.handle.seek(tbNewRowByte)

        while (self.handle.tell() <= file_size):
            if len(found) >= limit:
                break

            # read next byte (nb) without advancing current position
            cur_pos = self.handle.tell()
            nb = self.handle.read(1)
            self.handle.seek(cur_pos)

            if (nb == b'\xff'):
                break

            row_sz = int(np.frombuffer(self.handle.read(2), dtype=np.uint16))
            row_data = self.handle.read(row_sz)
            row_arr = row_data.split(b'\xfe')[:tbEntries]

            if comparisonOperator == "==" and row_arr[checkEntry].decode() != data:
                continue
            elif comparisonOperator == "!=" and row_arr[checkEntry].decode() == data:
                continue
            elif comparisonOperator == ">":
                try:
                    if int(row_arr[checkEntry].decode()) <= int(data):
                        continue
                except:
                    self.__setError("Invalid type for comparison operator")
                    return found
            elif comparisonOperator == "<":
                try:
                    if int(row_arr[checkEntry].decode()) >= int(data):
                        continue
                except:
                    self.__setError("Invalid type for comparison operator")
                    return found
            elif comparisonOperator == ">=":
                try:
                    if int(row_arr[checkEntry].decode()) < int(data):
                        continue
                except:
                    self.__setError("Invalid type for comparison operator")
                    return found
            elif comparisonOperator == "<=":
                try:
                    if int(row_arr[checkEntry].decode()) > int(data):
                        continue
                except:
                    self.__setError("Invalid type for comparison operator")
                    return found

            found_dict = {}
            
            for count, value in enumerate(tbEntryNames):
                found_dict[value] = row_arr[count].decode()
            
            found.append(found_dict)

        return found
        