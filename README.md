# another_tha_092022


The proposed solution used Sqlite to perform most of the data retrieval operations like joins and text search.

The use of SQlite simplifies the python code complexity and allows the use SQL in most operations. SQLite is a great tool that already has a lot of the optimizations required in data retrieval and text search. Also, because SQLite is a prepackaged solution the code maintenance is smaller compared with a more custom solution with other libraries.

Finally, there is a python layer that helps to ingest the data and create a facade that makes it easier to use this code.
