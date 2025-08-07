# BigDataAssignment
BigDataAssignment

**#Data source link**
https://www.kaggle.com/datasets/jamesmuema/retail-sales 

# Method 1: Jave MapReduce Workflow
1. Write Java files:
FinancialMapper.java
FinancialReducer.java
FinancialDriver.java
2. Ensure all import statements are correct
3. Compile Java files using Hadoop classpath:
mkdir bin
javac -classpath "<path_to_hadoop_libs>/*" -d bin Financial*.java
4. Package into JAR:
jar -cvf FinancialAnalysis.jar -C bin .
5. Upload JAR to S3
6. Upload dataset to S3
7. Create a new EMR cluster:
8. Add a custom JAR step:
Step type: Custom JAR
JAR location: s3://testinggroupassignment/FinancialAnalysis.jar
Arguments:
s3://testinggroupassignment/retail.csv s3://testinggroupassignment/output/
Action on failure: Terminate cluster or Continue
9. Go to the output S3 path:
s3://testinggroupassignment/output/
10. Download part-r-00000 and others to view results.

# Method 2: Python MapReduce Workflow
1. Prepare dataset, Mapper Python script, and Reducer Python script (make sure scripts are saved in LF format)
2. Create Amazon S3 Bucket
3. Upload the three documents into the created S3 bucket
4. Launch EMR cluster
5. Add Step at EMR Cluster and assign paths
6. Go to S3 bucket, output path
7. MapReduce files will be found at the output path (part-00000, part-00001, etc.)
8. Save results for analysis
