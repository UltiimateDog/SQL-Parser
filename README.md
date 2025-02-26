# BlazeDB - A Simple Database System

BlazeDB is a lightweight and simplified database engine designed for educational and experimental purposes. It supports a subset of SQL operations such as filtering (SELECT), sorting (ORDER BY), grouping (GROUP BY), aggregation (SUM), joins (INNER JOIN), and projections (SELECT specific columns). The project uses Java and simulates database operations on in-memory data.

## Features
- **SELECT**: Extract specific columns from tuples (Projection).
- **WHERE**: Filter tuples based on conditional expressions.
- **ORDER BY**: Sort tuples based on one or more columns.
- **GROUP BY**: Group tuples by specific columns.
- **SUM**: Aggregate data with the SUM operation.
- **JOIN**: Perform nested-loop joins between tables.
- **DISTINCT**: Eliminate duplicate tuples from query results.
- **PROJECTION**: Select specific columns from a tuple.

## JoinOperator Logic and Left-Deep Tree Construction

### JoinOperator Logic:

The `JoinOperator` is responsible for performing a nested loop join between two tables. It combines the tuples from the left child operator and right child operator according to the join condition provided in the WHERE clause. The logic of the `JoinOperator` can be summarized as follows:

1. **Left Tuple Retrieval**:  
   The `JoinOperator` starts by fetching the next tuple from the left child operator (i.e., the left table) using `getNextTuple()`. If no more tuples are available from the left table, the join process ends.

2. **Right Tuple Iteration**:  
   For each left tuple, the `JoinOperator` iterates over all the tuples of the right child operator (i.e., the right table). The nested loop ensures that each tuple from the left table is joined with all tuples from the right table.

3. **Combining Tuples**:  
   When a right tuple is found, it is combined with the left tuple to create a joined tuple. This joined tuple contains values from both the left and right tables.

4. **Join Condition Evaluation**:  
   After the two tuples are joined, the `ExpressionEvaluator` is used to evaluate the join condition (from the WHERE clause). If the join condition is satisfied (i.e., the condition evaluates to `true`), the joined tuple is returned as part of the result.

5. **Handling Non-Matching Tuples**:  
   If the join condition is not satisfied, the `JoinOperator` continues to the next tuple in the right table, and the process repeats.

6. **Resetting for New Left Tuple**:  
   Once all the right tuples have been checked for a given left tuple, the right child operator is reset (i.e., it starts over), and the next left tuple is fetched.

This process continues until all the left tuples have been joined with all the right tuples.

### Left-Deep Tree Construction:

The `Planner` is responsible for constructing the query execution plan from the parsed SQL query. The key here is that the `Planner` constructs a **left-deep tree** to process joins. A left-deep tree is a tree where every join is performed between the leftmost table and the next table in the query. The leftmost table is always processed first, and subsequent tables are joined one by one. This approach optimizes memory usage and ensures that each join is processed with minimal overhead.

Here’s how the `Planner` constructs a left-deep tree for joins:

1. **Starting with the First Table**:  
   The `Planner` first creates a `ScanOperator` for the first table in the query. This operator reads all tuples from the table.

2. **Applying Selection for the First Table**:  
   If there are non-join conditions (i.e., conditions that involve only the first table), a `SelectOperator` is applied to filter the rows of the first table before any joins are made.

3. **Processing Joins**:  
   The `Planner` then iterates over the remaining tables (if any). For each additional table, a `ScanOperator` is created to read tuples from the table, and a `SelectOperator` is applied to filter the rows based on non-join conditions.

4. **Performing the Join**:  
   The `JoinOperator` is applied between the current left operator (which is the result of previous joins) and the current right operator (the new table being added). This forms the left-deep tree where each join is connected in a chain-like structure.

5. **Final Query Plan**:  
   After all the tables are processed, the result is a chain of `JoinOperator` nodes connected to the initial `ScanOperator` for the first table. This left-deep tree represents the query execution plan.

### Optimizing Join Operations with Join Conditions

In the query plan, join operations can be optimized by extracting and evaluating the join conditions from the WHERE clause. Here's the approach that has been implemented for optimizing joins:

1. **Evaluating WHERE Clause Expressions**:
    - Each expression in the WHERE clause is evaluated to identify the conditions involving two tables.
    - These expressions are parsed and analyzed to determine which tables are used in each comparison.

2. **Extracting Non-Join Conditions**:
    - From the WHERE clause, the conditions that involve only **one table** (or **no tables**) are separated as non-join conditions. These conditions do not require a join and can be applied before the join operation.
    - Conditions involving **two tables** are considered potential join conditions, as these will be used to perform the join operation between the two tables.

3. **Applying Selection Before Join**:
    - Before performing the actual join, the non-join conditions are applied to **filter** the relevant tuples in the individual tables.
    - This reduces the size of the tuples to be joined, making the join operation more efficient by limiting the number of rows involved in the join.

4. **Evaluating the Full WHERE Clause After Join**:
    - Once the tuples are joined, the **full WHERE clause** is re-evaluated on the resulting joined tuple.
    - This ensures that only the rows that satisfy the complete set of conditions in the WHERE clause are returned.

By extracting and applying the join conditions before the join operation, this approach optimizes the overall query plan by reducing the number of tuples that need to be joined, thus improving the performance of join operations in complex queries.

## Query Plan Optimization

The database query execution plan is optimized using a tuple-by-tuple processing approach. Here's a breakdown of the key optimizations:

### 1. **Tuple-by-Tuple Processing**
- The `ScanOperator` reads one tuple at a time, starting with the first tuple in the table. This avoids reading the entire table into memory at once, which can be inefficient, especially for large datasets.
- This tuple-by-tuple approach ensures that only the required tuples are read and processed, improving memory usage and reducing unnecessary computation.

### 2. **Selection Before Join**
- The `SelectOperator` is applied **before** the `JoinOperator` for each tuple. This means that only the tuples that meet the selection criteria (i.e., the `WHERE` clause) are passed on to the join.
- By filtering out non-relevant tuples early on, this optimization prevents the join from being applied to tuples that would ultimately be discarded, minimizing unnecessary comparisons.

### 3. **Efficient Join Handling**
- The `JoinOperator` only considers tuples that have already passed the selection filter. This results in fewer tuples being compared during the join, further reducing unnecessary work.
- The join itself is performed **tuple by tuple**, meaning that only relevant pairs of tuples (those that pass the selection condition) are joined, avoiding the generation of large intermediate results.

### 4. **Order of Operations**
- The order of operations in the query execution is as follows:
    1. **Selection**: Filters out irrelevant tuples from each table.
    2. **Join**: Joins only the tuples that passed the selection condition.
    3. **Sorting**: Sorting is performed after the join, ensuring that only the relevant data is sorted.
    4. **Aggregation** and **Projection**: Applied to the filtered, joined data.
    5. **Distinct**: Removes duplicate tuples if needed.

This order ensures that only the tuples that have passed through the earlier operations are processed further, thus optimizing performance and memory usage.

### 5. **Minimizing Intermediate Results**
- By processing tuples one by one and applying operations as they go, no large intermediate results are generated. This prevents the system from storing unnecessary data in memory, ensuring that only the relevant tuples are kept at any time.

### 6. **Avoiding Full Table Scans**
- Since the `ScanOperator` reads and processes tuples one by one, the entire table is never loaded into memory at once. This is a significant optimization for large tables, as it ensures that the system doesn’t waste memory on data that isn’t required for the query.

### 7. **Efficient Execution of Additional Operations**
- **Sorting**, **aggregation**, **projection**, and **distinct** operations are performed **after** the selection and join. This means that the operations are applied to a smaller set of tuples that have already been filtered and joined, making them more efficient.

### Why This Approach is Optimized

- **Early Filtering**: The fact that selection is applied before the join ensures that the number of tuples involved in the join is minimized, which makes the process more efficient.
- **Minimizing Intermediate Results**: By processing tuples individually and applying operations as they go, we avoid generating large intermediate result sets.
- **Join Optimization**: The join is applied only on relevant tuples, which reduces the number of unnecessary comparisons.
- **Efficient Memory Usage**: The system avoids loading entire tables into memory, which is particularly important when dealing with large datasets.

## Setup

To run this project, you'll need Java and Maven. The project also uses the Lombok library for some code generation and reduces boilerplate code.

### Prerequisites

- Java 11 or newer
- Maven
- IntelliJ IDEA (or another IDE that supports Maven)

### Lombok Configuration in IntelliJ IDEA

Lombok can sometimes cause issues with IntelliJ IDEA, so here’s how to configure it properly:

1. **Install Lombok Plugin**:
    - Open **Settings** (File > Settings) in IntelliJ IDEA.
    - Navigate to **Plugins**.
    - Search for **Lombok** and install the plugin.
    - Restart IntelliJ IDEA after installation.

2. **Enable Annotation Processing**:
    - Open **Settings** (File > Settings) again.
    - Navigate to **Build, Execution, Deployment > Compiler > Annotation Processors**.
    - Check **Enable annotation processing**.
    - Select **Obtain processors from project classpath**.
    - Set **Module output directory** for annotation processing.

3. **Add Lombok Dependency**:
    - Ensure that Lombok is included in the `pom.xml` file under `<dependencies>` section:

    ```xml
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <version>1.18.24</version> <!-- Use the latest version -->
        <scope>provided</scope>
    </dependency>
    ```

4. **Rebuild the Project**:
    - In IntelliJ IDEA, go to **Build > Rebuild Project** to ensure everything compiles correctly.
