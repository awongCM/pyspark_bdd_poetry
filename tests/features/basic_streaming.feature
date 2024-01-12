Feature: Basic Streaming Example

# For When and Then fixtures, check the corresponding step file for more clarity
  Scenario: Count some words with Spark Streaming
   Given a spark streaming context
   When I provide some a streaming list of multi-dimensional words below
   Then I expect the streaming words to display array of unique word count
