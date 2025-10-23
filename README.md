# JDBC_Similarity

This repository contains the solution for Assignment 1 in the Big Data course.  
The project includes SQL functions for similarity calculation and a Java class that interacts with the database using JDBC.

---

## Project Structure

MediaSimilarityManager.java   – Java class with JDBC implementation  
media_similarity_queries.txt  – SQL schema and functions (DDL + similarity functions)  

---

## Part 1: SQL (media_similarity_queries.txt)

### Database Schema

Table MediaItems  
MID (NUMERIC(11,0)) – Primary key  
PROD_YEAR (NUMERIC(4,0)) – Production year  
TITLE (VARCHAR(100)) – Media item title  

Table Similarity  
MID1 (NUMERIC(11,0)) – Primary key, foreign key → MediaItems.MID  
MID2 (NUMERIC(11,0)) – Primary key, foreign key → MediaItems.MID  
SIMILARITY (REAL) – Similarity value in range [0,1]  

### SQL Functions

MaximalDistance()  
Returns the maximal distance between two media items, defined as the absolute difference between their production years.  

SimCalculation(MID1, MID2, maxDist)  
Calculates similarity between two media items:  
similarity(a, b) = 1 - (|year(a) - year(b)| / maximalDistance)  

---

## Part 2: Java (MediaSimilarityManager.java)

The Java class provides the following methods:

Constructor  
Receives DB username and password.  
Defines the connection URL (does not connect immediately).  

fileToDataBase(String path)  
Reads a CSV file (title, year) and inserts the records into MediaItems.  

calculateSimilarity()  
Uses the SQL functions to calculate similarity for every pair of items.  
Inserts or updates the values in the Similarity table.  

printSimilarities(long mid)  
Prints all media items with similarity ≥ 0.3 to the given MID.  
Results are sorted ascending by title.  
Format:  
Title – SimilarityValue  

---

## How to Run

1. Import media_similarity_queries.txt into your SQL Server database.  
2. Compile the Java class:  
   javac MediaSimilarityManager.java  
3. (Optional) Create a main function for testing. Example:  

```java
public static void main(String[] args) throws Exception {
    MediaSimilarityManager manager = new MediaSimilarityManager("username", "password");
    manager.fileToDataBase("films.csv");
    manager.calculateSimilarity();
    manager.printSimilarities(1L);
}
