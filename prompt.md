
## Initial prompt

I need to build a Spark Python data source that will allow to write and read to/from
AWS DynamoDb database.  Brainstorm and prepare a plan for implementing a python data 
source with name `dynamodb`. 

Let's split the development into a few phases:

- only write to DynamoDB (both batch and streaming). When writing data, check that data
  contains at least primary key of DynamoDB table. When writing, perform data mapping if
  necessary (like, string -> uuid).
- allow to delete rows when writing when there are specific options are provided: name of
  the column that is deletion flag (it shouldn't be written into the table), and value
  that is the deletion flag.
- Doing batch reading from DynamoDB. If schema isn't provided, it should be obtained via
  DynamoDB driver mapping unsupported types (like, uuid -> string, etc.).  If schema is
  specified, check that it's matching to the structure of DynamoDB table and return only
  specific columns.
- Do not implement streaming reads.


