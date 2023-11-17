----- Introduction -----

SejDB is database management software written in Python which prioritises a small file size and speed over anything else.

Advantages:
- Databases are small
- It is (somewhat) fast
	- Creates 10k tables in 0.34s
	- Inserts 10k rows randomly in 88.46s
	- Inserts 10k rows into one table in 0.35s
	- Searches for 5 rows randomly distributed throughout db in 0.05s
	- Deletes 10k rows in 0.38s

- Open source
- No special database language required, everything is done via function calls

This DB was just a small hobby and primarily coded by 1 person so there may be many bugs which may cause problems.
Only one process can access a .sdb file, however, you could code a server which uses the sejdb.py import, and handles many clients manipulating the database file.

----- sejdb/sejdb.py -----

This file contains all the functions required to manipulate a database file (.sdb).

----- sejdb/README.txt -----

This file contains a list of all the functions provided by the sejdb.py file, and what they do.

----- sejdb/example.py -----

This file shows the basics of using the sejdb.py file.