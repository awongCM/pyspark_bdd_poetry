Feature: Basic JSON DataFrame Read (Simple Transformation)

 Scenario: Read JSON data with Spark
   Given a spark session
   When I read some JSON data coming into Spark that has multi-lines
   Then I expect to see same data appearing in dataframe format it came with
   And It gets transformed with its name column as lower-case characters

