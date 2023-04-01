# Database-Coursework

## A University Databases Coursework that consists of 3 parts:
  1. Design and develop an Entity-Relationship Diagram (ERD)
  2. Write relational schema and integrity constraints for your relations. Provide the relevant SQL DDL statements to implement your ERD.
  3. Create a java library containing methods to read given .csv files that match the supplied example 􀃞les and generate the relevant SQL insert statements to populate the database, and Create a set of SQL queries to answer 5 given questions

## Prerequisite to run the code in Milestone 3
  - You must have Java Installed on your Machine to compile the code
  - You must always keep "sqlite3.exe" file in the "Milestone 3" Folder OR have sqlite3 installed on your machine
  - You must always keep "sqlite-jdbc-3.20.1.jar" file in the "Milestone 3" Folder
  - You must have the "Combat.csv", "Customers.csv" & "Items.csv" files to populate the Database (Also you can change the contents of the files to suit your case)

## How to run the code in Milestone 3
  - Clone this repo into your local machine
  - Go to the "Milestone 3" Folder
  - Based on the Operating System you are using use the appropriate command below:
    - Windows: java -classpath ".;sqlite-jdbc-3.20.1.jar" DatabaseLibrary.java
    - Linux: java -classpath ".:sqlite-jdbc-3.20.1.jar" DatabaseLibrary.java
