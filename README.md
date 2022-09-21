# another_tha_092022


The proposed solution used Sqlite to perform most of the data retrieval operations like joins and text search.

The use of SQlite simplifies the python code complexity and allows to use SQL in most operations. SQLite is great tool that already have a lot of the optimizations required in data retrieval and text search. Also, because SQLite is prepackaged solution the code maintenance is smaller compared with a more custom solution with other libraries.

Finally, there is python layer that help to ingest the data and create a facade that makes easier use this code.
