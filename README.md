# Sessions

This is an exercise that consists of creating an algorithm that can display the average time of all user sessions on a platform. Where we have this information:

- User = Identifier of the user on the platform
- Time = Timestamp of the moment when the session starts or ends.
- Status= Identifier that can be Open or Close.

The input for this process can come from any source, but for this first part of the exercise we will assume that the analysis is performed in batch mode and is received through a csv file containing the log of all user session openings and closings.

Example: 

```json
User 1,1587383460,Open
User 1,1587383500,Open
User 1,1587383520,Close
User 2,1587383460,Open
User 2,1587383580,Close
User 1,1587383700,Open
User 2,1587387660,Open
User 3,1587383460,Open
User 1,1587383940,Close
User 2,1587387900,Close
User 3,1587383580,Close
User 4,1587390780,Open
User 4,1587390840,Open
```

In a future version of this exercise I will be performing these calculations for a streaming process where the source could for example be a publish and subscribe system and where the average would vary with the new information coming through the stream.

To solve this problem we will first make some assumptions: 
- The data set is not ordered.
- It is assumed that for each user there will be a session record if the opening and closing records are found. 
- In case only one of the two is found, the session will not be counted (later on we can make changes and assume a default session time in case we don't have any of the opening and closing events, but I will do that in a future version).
- In case an Opening value is found followed by an Opening value, the higher one will be taken as the comparison point for a next closing and the first one will be discarded.
- It is assumed that all the records are complete in their attributes that is to say that it does not have nulls (In another version we can control the nulls in a data cleaning process).

## Process

To solve this problem I will do it with the first approach I can think of and in later versions it will be optimized, this in order to iterate on solutions and improve the system. I am sure that there are many better ways to do this exercise but we will find them on the way.
In this case I will solve it in three different ways: Using basic Python, using Pandas and using Pyspark. However first we can define the general process for any solution.

Steps:
1. Define that the objective is to obtain the average of all user sessions of a platform, where this average is calculated as: 
Average_Sessions = Sum (Time of Sessions) / Number of Sessions
2.  Read the file with the data
3. Sort the data by user and session time. We do this to be able to group by user and look for the openings and closings of each user.
4. Calculate the time of duration of each pair of elements from the opening and closing. 
5. Perform the calculation defined in point 1 by adding up all the session durations and dividing them by the number of sessions.

Now the solution for each proposed form is found in the file: [Sessionalize.py](https://github.com/faiderfl/Sessions/blob/main/Sessionalize.py) where we create a method for each approach and call them all in the execution part of the system. 

Now the solution for each proposed form is found in the file: Sessionalize.py where we create a method for each approach and call them all in the execution part of the system. 

**Python:** 

- We read the file [Sessions.csv](https://github.com/faiderfl/Sessions/blob/main/Sessions.csv) and save it in a list of sessions.

- We create a hash table (Dictionary in Python) where the key will be the union of the user and the time of the event making it unique because you can not have two records for the same user in the same second and the value will be the Status (Open or Close).

- We order the dictionary by key (User-Time)

- We convert the dictionary into a list where each element will be of type [Key, Value].

- We go through the list and identify for each record if the Status == 'Open' and the next record is still from the same user and the Status == 'Close' in which case a session is counted and the duration of the session is stored in a list.

- Then the average is calculated by adding the values of the session durations and dividing by the number of sessions. 

  

  This solution is the fastest but not scalable.

**Pandas:**

- We read the file [Sessions.csv](https://github.com/faiderfl/Sessions/blob/main/Sessions.csv) and save it in a data frame.

- Sort the data frame by User and Time

- We create a new column called duration that will be calculated for each record in the data frame if the Status == 'Open' and the next record is still from the same user and the Status == 'Close' in which case a session is counted.

- Then the sessions are displayed and the average is calculated by adding the values of the session durations and dividing by the number of sessions. In pandas it is as simple as using sessions.duration.mean()

  

  This solution is lower than Python but is more simple.

**Pyspark:**

- We create a structure for a data frame that will contain the data.
- We read the [Sessions.csv](https://github.com/faiderfl/Sessions/blob/main/Sessions.csv) file and save it in the data frame.
- Sort the data frame by User and Time
- Create a new column called Session using a window function on the partitioning by user sorted by time. Where we look for the lag (value of the immediately previous record) of each record that is a closure. Then we can calculate a new Duration column by subtracting the time of the Open and Close events.
- Then the sessions are displayed and the average is calculated by adding the values of the session durations and dividing by the number of sessions. 

This last solution can also be calculated using Spark SQL which will be discussed in a future version.
I have to say that this is my preferred solution because it will be the one that best scales with the number of users and sessions because when using Spark a distributed system in memory will be much more efficient than the versions of Python that uses data structures but reads the file sequentially. and Pandas that although it is excellent will depend on a machine where the calculations are performed contrary to Spark that runs in parallel in a cluster.

​	This solution is the lowest but it is because it needs to execute the Spark session but is better in terms of scalability and complexity.

**Conclusion**

In conclusion, this process allows us to analyze how to calculate averages in the data by navigating the data structures and allowing us to review data query concepts.

For a next version we can make many changes, such as optimizing the Python process, review whether to continue with the first data dictionary approach. Especially because it is the algorithm with more complexity, more number of cycles and although it is for the moment the fastest, it certainly does not scale well to have 4 cycles and a sorting.

Another idea could be to make a new solution using pure SQL or Spark SQL.
We can also try to have much more data to compare processing times and resource usage. 



**Console log:**

[['User 1-1587383460', 'Open'], ['User 1-1587383500', 'Open'], ['User 1-1587383520', 'Close'], ['User 1-1587383700', 'Open'], ['User 1-1587383940', 'Close'], ['User 2-1587383460', 'Open'], ['User 2-1587383580', 'Close'], ['User 2-1587387660', 'Open'], ['User 2-1587387900', 'Close'], ['User 3-1587383460', 'Open'], ['User 3-1587383580', 'Close'], ['User 4-1587390780', 'Open'], ['User 4-1587390840', 'Open']]
[20, 240, 120, 240, 120]
The average time of all session is: 148.0

`Elapsed time: 0.0009975433 seconds.`



​      User          Time Status  duration
2   User 1  1.587384e+09  Close      20.0
8   User 1  1.587384e+09  Close     240.0
4   User 2  1.587384e+09  Close     120.0
9   User 2  1.587388e+09  Close     240.0
10  User 3  1.587384e+09  Close     120.0
The average time of all session is:

`Elapsed time: 0.0468747616 seconds.`





+------+----------+--------+
|  User|      Time|Duration|
+------+----------+--------+
|User 2|1587383580|     120|
|User 2|1587387900|     240|
|User 3|1587383580|     120|
|User 1|1587383520|      20|
|User 1|1587383940|     240|
+------+----------+--------+

The average time of all session is:
+-------------+
|avg(Duration)|
+-------------+
|        148.0|
+-------------+

`Elapsed time: 7.4767644405 seconds.`



*Using radon to review complexity:*

PS D:\Github\Sessions>  radon cc .\Sessionalize.py --total-average
.\Sessionalize.py
    F 11:0 sessions_python - B
    F 45:0 sessions_pandas - A
    F 57:0 sessions_spark - A

3 blocks (classes, functions, methods) analyzed.
Average complexity: A (3.0)

| CC score | Rank | Risk                                   |
| :------- | :--- | :------------------------------------- |
| 1 - 5    | A    | low - simple block                     |
| 6 - 10   | B    | low - well structured and stable block |