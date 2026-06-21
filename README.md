# spark-assignment-5
# Assignment on DataFrames in Apache Spark

## 1. Purpose
This assignment will illustrate and highlight some core knowledge of the architecture of Apache Spark and how it will help you to see some of the key limitations of traditional Map/Re-Map and give you practice building a data-processing pipeline by developing an understanding of how to use PySpark DataFrames along with common data manipulation concepts and techniques for cleaning, transforming and aggregating your data.

## 2. Basic Concepts and Architecture
### Limitations of MapReduce vs. Benefits of Spark
* **Disadvantages of MapReduce**: Traditional MapReduce in Hadoop have too much Disk-IO. There is a Write of all Intermediate Data Sets to Disk Between Each Sequential Map Operation and Reduce Operation. This high amount of I/O and the associated Network Overhead limits the ability of Traditional MapReduce to Perform Multi-pass Iterative Jobs Such As Machine Learning Algorithms or Complex Data Pipelines.
* **Benefits of Spark**: Apache Spark uses **In-Memory Processing** whereby data is Stored in Memory between Operations in RAM instead of Writing Intermediate Results to a Physical Disk. Since there is No Repetitive Disk Reads/Writes for Each time the Multi-pass Workflow is executed, Traditional MapReduce is ~100x Slower than Apache Spark.

What is Spark DataFrames and Immutability?
- DataFrames are collections of distributed datasets organized in a set of named columns. This structure provides Spark with a defined API to optimize execution plans prior to any execution of actual computations. 
- Spark DataFrames are completely immutable; once they are created their data structure cannot be modified. For example, every time we transform (i.e., filter, drop nulls, or cast) a DataFrame, we will create a new DataFrame with the results. This way it preserves the historical Lineage Graph of the DataFrame, so if a node fails, Spark can automatically rebuild the missing partitions for that DataFrame.

What are Wide Transformations and Shuffle Operations?
- A narrow transformation consists of operations (e.g., filter(), select(), withColumnRenamed()) that require one input partition into one output partition. Because the transformed data stays in the same partition, there is no need for network movement.
- A wide transformation consists of operations that require data from multiple partitions throughout an entire cluster to be distributed and grouped together by key (e.g., groupBy(), distinct(), orderBy()).
- The term used to describe the physical operation of moving and redistributing data amongst different cluster nodes during a wide transformation is known as shuffling. The shuffling of data makes Spark jobs very expensive, both in disk I/O and network usage.

## 3. Pipeline Design & Implementation Steps
The processing pipeline that is implemented in the Kaggle notebook contains the following defined engineering stages:
1. **Spark Context Initialisation:** A consistent spark session was established for the purpose of executing distributed evaluations of the local dataset.
2. **Data Ingestion Input:** The ingestion occurs from a sample file called "Sample-Superstore.CSV". The ingestion command uses the options "quote = '" and 'escape = '"' to ensure that there will be no problems caused by nested quotes in the product's name (such as 25"x30" ream) when attempting to evaluate records on a locked basis.
3. **Data Cleaning:** The cleaning of the raw data matrix occurred using ".dropDuplicates()" to remove the occurrence of identical rows within the raw data and from .na.fill() to account for the existence of missing data in critical transaction dimensions.
4. **Data Normalisation & Schema Mapping:** Column headers have been normalised through the removing of spaces and special hyphens in the column name (e.g. Sub Category becomes Sub_Category) and changing/explicitly casting data types to double precision numbers from structural text data types (e.g. changing sales and profit from text to double precision).
5. **Data Isolation (Field Filtering):** Apply condition compound filtering to isolate transactions for the given region in the given category (e.g. Region == West and Category == Technology).
6. **Data Synthesis (Aggregation):** Aggregation of regional performance metrics through a "GroupBy("Segment","Region")" to sum up all transactional records and evaluate performance metrics associated with segments in each region.


## 4. Noteworthy Observations and Insights
* **The Importance of Safer Parsing:** The typical error that results from parsing a CSV file when the text contains unescaped quotation marks — e.g., height in inches — will produce a parse error due to the lack of appropriate escape or quote parameters specified in the CSV file specification. By providing explicit values for the `quote` or `escape` parameters, you can avoid errors from invalid entries during downstream schemata casting.
* **The Lazy Evaluation of RDDs:** When the CSV file was loaded into memory, no CSV data was read or parsed immediately. Instead, the operational steps were not executed until an **Action** on the RDD was invoked (e.g., `.show()`). Through lazy evaluation, Spark was able to produce a highly optimized execution plan based on the available rows.
* **The Partitioning of DataFrame Outputs:** Due to Spark operating as a distributed compute engine, a DataFrame output will produce a collection of parallel or partitioned data chunks (i.e., `part-00000...`) in a folder rather than a single monolithic output file.
