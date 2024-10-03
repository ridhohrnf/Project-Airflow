# DE Coding Test

## Resources

There are three tables, which are `store`, `cashier`, and `sales`

### Store
Schema: StoreId, StoreName, Location

Save as .xslx file

### Cashier
Schema: CashierId, Name, Email, Level

You are given the event logs of this tables in the respective directory name under `cashier_data` directory.
Logs can be new data or updates, logs are sorted by timestamp (yyyymmddhhmmss). The key used is CashierId.

### Sales
Schema: SaleID, StoreID, CashierId, Amount

Containing the total sales made by each cashier in each store.
Old sales data, stored in .parquet format in the `sales_data` directory.
Then there is new sales data in the `new_sales_data` directory.

## Task

1. Create dimensional data modeling for all source tables
2. Create several Data Marts from the dimensional data modeling that has been created
3. Create an ETL for each tables, and data mart scheduler every day
4. Create pipeline of all the ETL processes that have been created

### Additional task:
- Make ETL idempotent
- Make the pipeline can handles back-dating and an initial load
- Add monitoring or logging system


## Submission

To submit your tasks, simply create the necessary implementation code in given `solution` directory.
You may want make this directory a git repository and checkpoint your work periodically here.

In addition to your core logic codebase, please also include scripts to deploy your code to a Docker container.
Your code has to be deployed in **local Docker container**. Please also include the necessary files (such as `Dockerfile` and automation scripts)
to deploy and run your code inside a Docker container.

Please also include a `README.md` inside the `solution` directory to give a summary of your submitted solution,
the thinking behind your implemented solution, and how to run your solution end-to-end in a Docker container
to get the desired result as stated above. 