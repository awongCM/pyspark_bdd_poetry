Feature: Basic Word Count Example

 Scenario: Count some words with Spark
   Given a spark session
   When I count the words in "the complete works of Shakespeare"
   Then the number of word is '5'
