# RDBMS

Relational database management system (RDBMS) is the most common database management system. In this project, I will use Python to design a RDBMS from scratch according to the project guideline. The system will implement all the features mentioned in the project guideline and have a new language that is different from the existing database languages. Through this project, I have deepened my understanding of this course and improved my ability to solve problems through programming.

## Files Description

### Code
- database.py: defines a Database class, which implements creating / dropping tables, and inserting / deleting / updating / querying records, etc.
- engine.py: defines an Engine class, which implements creating / dropping databases and interaction with users, etc.
- main.py: the entrance of the program.

### Data
- databases/: The folder where all databases in this RDBMS are stored.
- databases/iris/: The folder where the iris database is stored (in order to demonstrate the query function more conveniently, I divided the original iris data set into two tables and stored them in the database in advance).
- databases/iris/metadata.jsonl: The .jsonl file that stores metadata for the iris database.
- databases/iris/Attribute.jsonl: The .jsonl file that stores the Attribute table of the iris database.
- databases/iris/Kind.jsonl: The .jsonl file that stores the Kind table of the iris database.

## Running Environment
This RDBMS can run in the following environment (due to time constraints, I have not tested whether it can run normally in other software and hardware environments. If you cannot run it normally, please contact me at sunchenn@usc.edu and I will do my best to help you :)).

### Hardware
- computer: MacBook Pro 14-inch, 2021
- cpu: Apple M1 Pro
- memory: 16 GB (Although I don't think it needs to consume a lot of memory to run this RDBMS)

### Software
- Python 3.9.6
- dependent libraries (These are the standard libraries of Python 3.9.6. If you don’t have them, you need to install them using pip3)
-- json
-- typing
-- os
-- shutil
-- functools
-- tempfile

## Usage Examples
I will show you how to use this RDBMS in the form of menu interaction through three examples (see the report for screenshots of these three examples).

### Start
First, you need to enter the terminal and set the current path to RDBMS (if your current path is already RDBMS, you do not need to perform this step):
```
cd <path-to-RDBMS>
```

Next, you need to start the program:
```
python3 main.py
```

Then, you can complete the following tasks through the interactive menu (for ease of use, the menu has given a large number of hints, you only need to enter the relevant instructions according to the hints):

### Example 1
query: find iris samples that belong to Iris-versicolor, return ids only in descending order (using select, project, sort).

```
>>>Chenning_DBMS: Please enter which function about database do you want to use? Options include: "1" for "create database", "2" for "drop database", "3" for "show database names", "4" for "use database". Enter "exit" to finish process.
Your input: 4
>>>Chenning_DBMS: Please enter the name of the database you want to use. Enter "exit" to return to main menu.
Your input: iris        
>>>Chenning_DBMS: Please enter which function about table do you want to use? Options include: "1" for "create table", "2" for "drop table", "3" for "show table names", "4" for "use table". Enter "exit" to return to main menu.
Your input: 4
>>>Chenning_DBMS: Please enter which function about record do you want to use? Options include: "1" for "insert record", "2" for "update record", "3" for "delete record", "4" for "query record". Enter "exit" to return to main menu.
Your input: 4
>>>Chenning_DBMS: Please enter the name of the table you want to query record. Enter "exit" to return to main menu.
Your input: Kind
>>>Chenning_DBMS: Do you want to do cross product? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: n
>>>Chenning_DBMS: Do you want to do theta inner join? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: n
>>>Chenning_DBMS: Do you want to do selection? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: y
>>>Chenning_DBMS: Please enter conditions one at a time to select records that you want. Enter "stop" to stop adding conditions. Enter "exit" to return to main menu.
Your input: Kind.species == "Iris-versicolor"
>>>Chenning_DBMS: Please enter conditions one at a time to select records that you want. Enter "stop" to stop adding conditions. Enter "exit" to return to main menu.
Your input: stop
>>>Chenning_DBMS: Do you want to do grouping by and aggregation? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: n
>>>Chenning_DBMS: Do you want to do projection? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: y
>>>Chenning_DBMS: Please enter all fields that you want to remain, using white space to separate fields. Enter "exit" to return to main menu.
Your input: Kind.id
>>>Chenning_DBMS: Do you want to do sorting using sort merge algorithm? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: y
>>>Chenning_DBMS: Please enter the field that you want to sort by. Enter "exit" to return to main menu.
Your input: Kind.id
>>>Chenning_DBMS: Please enter whether you want to sort in ascending order or not. Enter "a" for ascending order. Enter "d" for descending order. Enter "exit" to return to main menu.
Your input: d
>>>Chenning_DBMS: Please enter the chunk size when using sort merge algorithm. Enter "exit" to return to main menu.
Your input: 5
>>>Chenning_DBMS: Do you want to show the query result? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: y
>>>Chenning_DBMS: Please enter the number of records that you want to show. Enter "all" to show all records. Enter "exit" to return to main menu.
Your input: all
```

result: 

```
{"Kind.id": 100}
{"Kind.id": 99}
{"Kind.id": 98}
{"Kind.id": 97}
{"Kind.id": 96}
{"Kind.id": 95}
{"Kind.id": 94}
{"Kind.id": 93}
{"Kind.id": 92}
{"Kind.id": 91}
{"Kind.id": 90}
{"Kind.id": 89}
{"Kind.id": 88}
{"Kind.id": 87}
{"Kind.id": 86}
{"Kind.id": 85}
{"Kind.id": 84}
{"Kind.id": 83}
{"Kind.id": 82}
{"Kind.id": 81}
{"Kind.id": 80}
{"Kind.id": 79}
{"Kind.id": 78}
{"Kind.id": 77}
{"Kind.id": 76}
{"Kind.id": 75}
{"Kind.id": 74}
{"Kind.id": 73}
{"Kind.id": 72}
{"Kind.id": 71}
{"Kind.id": 70}
{"Kind.id": 69}
{"Kind.id": 68}
{"Kind.id": 67}
{"Kind.id": 66}
{"Kind.id": 65}
{"Kind.id": 64}
{"Kind.id": 63}
{"Kind.id": 62}
{"Kind.id": 61}
{"Kind.id": 60}
{"Kind.id": 59}
{"Kind.id": 58}
{"Kind.id": 57}
{"Kind.id": 56}
{"Kind.id": 55}
{"Kind.id": 54}
{"Kind.id": 53}
{"Kind.id": 52}
{"Kind.id": 51}
50 record(s)
```

### Example 2
query: for each kind of iris, find the shortest sepal length and the largest petal width (join, groupby, aggregate).

```
>>>Chenning_DBMS: Please enter which function about database do you want to use? Options include: "1" for "create database", "2" for "drop database", "3" for "show database names", "4" for "use database". Enter "exit" to finish process.
Your input: 4
>>>Chenning_DBMS: Please enter the name of the database you want to use. Enter "exit" to return to main menu.
Your input: iris
>>>Chenning_DBMS: Please enter which function about table do you want to use? Options include: "1" for "create table", "2" for "drop table", "3" for "show table names", "4" for "use table". Enter "exit" to return to main menu.
Your input: 4
>>>Chenning_DBMS: Please enter which function about record do you want to use? Options include: "1" for "insert record", "2" for "update record", "3" for "delete record", "4" for "query record". Enter "exit" to return to main menu.
Your input: 4
>>>Chenning_DBMS: Please enter the name of the table you want to query record. Enter "exit" to return to main menu.
Your input: Attribute
>>>Chenning_DBMS: Do you want to do cross product? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: n
>>>Chenning_DBMS: Do you want to do theta inner join? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: y
>>>Chenning_DBMS: Please enter the name of the table you want to do theta inner join with. Enter "stop" to stop doing theta inner join. Enter "exit" to return to main menu.
Your input: Kind
>>>Chenning_DBMS: Please enter conditions one at a time to decide how to do theta inner join. Enter "stop" to stop adding conditions. Enter "exit" to return to main menu.
Your input: Attribute.id == Kind.id
>>>Chenning_DBMS: Please enter conditions one at a time to decide how to do theta inner join. Enter "stop" to stop adding conditions. Enter "exit" to return to main menu.
Your input: stop
>>>Chenning_DBMS: Please enter the name of the table you want to do theta inner join with. Enter "stop" to stop doing theta inner join. Enter "exit" to return to main menu.
Your input: stop
>>>Chenning_DBMS: Do you want to do selection? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: n
>>>Chenning_DBMS: Do you want to do grouping by and aggregation? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: y
>>>Chenning_DBMS: Please enter all fields that you want to group by, using white space to separate fields. Enter "exit" to return to main menu.
Your input: Kind.species
>>>Chenning_DBMS: Please enter all fields that you want to aggregate, using white space to separate fields. Enter "exit" to return to main menu.
Your input: Attribute.sepalLengthCm Attribute.petalWidthCm
>>>Chenning_DBMS: Please enter the lambda function that you want to use to aggregate "Attribute.sepalLengthCm". Enter "exit" to return to main menu.
Your input: lambda x, y: min(x, y)
>>>Chenning_DBMS: Please enter the lambda function that you want to use to aggregate "Attribute.petalWidthCm". Enter "exit" to return to main menu.
Your input: lambda x, y: max(x, y)
>>>Chenning_DBMS: Do you want to do projection? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: n
>>>Chenning_DBMS: Do you want to do sorting using sort merge algorithm? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: n
>>>Chenning_DBMS: Do you want to show the query result? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.
Your input: y
>>>Chenning_DBMS: Please enter the number of records that you want to show. Enter "all" to show all records. Enter "exit" to return to main menu.
Your input: all
```

result: 

```
{"Kind.species": "Iris-setosa", "Attribute.sepalLengthCm": 4.3, "Attribute.petalWidthCm": 0.6}
{"Kind.species": "Iris-versicolor", "Attribute.sepalLengthCm": 4.9, "Attribute.petalWidthCm": 1.8}
{"Kind.species": "Iris-virginica", "Attribute.sepalLengthCm": 4.9, "Attribute.petalWidthCm": 2.5}
3 record(s)
```

### Example 3
- create a database called dsci551.
- create a table called Student in database dsci551, which contains 3 fields: Student.sid(int), Student.name(str), Student.age(int).
- insert a record {"Student.id":11,"Student.name":"Chenning","Student.age":23} in table Student.


```
>>>Chenning_DBMS: Please enter which function about database do you want to use? Options include: "1" for "create database", "2" for "drop database", "3" for "show database names", "4" for "use database". Enter "exit" to finish process.
Your input: 1
>>>Chenning_DBMS: Please enter the name of the database you want to create. Enter "exit" to return to main menu.
Your input: dsci551
>>>Chenning_DBMS: Please enter which function about database do you want to use? Options include: "1" for "create database", "2" for "drop database", "3" for "show database names", "4" for "use database". Enter "exit" to finish process.
Your input: 4
>>>Chenning_DBMS: Please enter the name of the database you want to use. Enter "exit" to return to main menu.
Your input: dsci551
>>>Chenning_DBMS: Please enter which function about table do you want to use? Options include: "1" for "create table", "2" for "drop table", "3" for "show table names", "4" for "use table". Enter "exit" to return to main menu.
Your input: 1
>>>Chenning_DBMS: Please enter the name of the table you want to create. Enter "exit" to return to main menu.
Your input: Student
>>>Chenning_DBMS: Please enter the field-data_type pairs of the table you want to create. Enter "exit" to return to main menu.
Your input: {"Student.id":"int","Student.name":"str","Student.age":"int"}
>>>Chenning_DBMS: Please enter which function about database do you want to use? Options include: "1" for "create database", "2" for "drop database", "3" for "show database names", "4" for "use database". Enter "exit" to finish process.
Your input: 4
>>>Chenning_DBMS: Please enter the name of the database you want to use. Enter "exit" to return to main menu.
Your input: dsci551
>>>Chenning_DBMS: Please enter which function about table do you want to use? Options include: "1" for "create table", "2" for "drop table", "3" for "show table names", "4" for "use table". Enter "exit" to return to main menu.
Your input: 4
>>>Chenning_DBMS: Please enter which function about record do you want to use? Options include: "1" for "insert record", "2" for "update record", "3" for "delete record", "4" for "query record". Enter "exit" to return to main menu.
Your input: 1
>>>Chenning_DBMS: Please enter the name of the table you want to insert record. Enter "exit" to return to main menu.
Your input: Student
>>>Chenning_DBMS: Please enter the record want to insert. Enter "exit" to return to main menu.
Your input: {"Student.id":11,"Student.name":"Chenning","Student.age":23}
```

result: 

A folder named dsci551/ will appear under the databases/ folder, which contains two files:
- metadata.jsonl: {"Student": {"Student.id": "int", "Student.name": "str", "Student.age": "int"}}
- Student.jsonl: {"Student.id": 11, "Student.name": "Chenning", "Student.age": 23}

### Exit
You can exit the program at any time by typing "exit".

```
>>>Chenning_DBMS: Please enter which function about database do you want to use? Options include: "1" for "create database", "2" for "drop database", "3" for "show database names", "4" for "use database". Enter "exit" to finish process.
Your input: exit
```

I hope these examples help you run this RDBMS successfully. If you still have questions, please contact me at sunchenn@usc.edu and I will do my best to help you :).


