# ActiveDB
A PoC log in DB manager that uses query counts and sorting to keep the most relevant users at the top of the DB.

# Concept

The idea behind this was that there are "power" users that may log into an application more than others such as dead accounts, or temperary users. This uses a query count to determine who logs in more and sorts the DB via that. So if a user is logging in or doing many actions at either a particular moment or through an extended period it would be at the top of the records. 

# Query times (1 million entries and 10 million requests) approx 100mb of username/password/query data...

- Average request time: 0.00000068342297077179 seconds
- Maximum request time: 0.00280690193176269531 seconds //most likely first time requests and queries before python caching...
- Average reorganize time: 3.73611903190612792969 seconds
- Maximum reorganize time: 3.96744394302368164062 seconds

# Output

Starting user generation...
Batch 0 to 100000 added.
...
Batch 900000 to 1000000 added.
All users generated and saved.
Generated 1000000 users in 21.48 seconds.
User generation complete!
Starting traffic simulation...
Reorganizing database after 1000000 queries...
Reorganizing database...
Database reorganized.
...
Reorganizing database after 9000000 queries...
Reorganizing database...
Database reorganized.
Simulated 10000000 queries in 63.09 seconds.
Traffic simulation complete!
Average request time: 0.00000068342297077179 seconds
Maximum request time: 0.00280690193176269531 seconds
Average reorganize time: 3.73611903190612792969 seconds
Maximum reorganize time: 3.96744394302368164062 seconds
