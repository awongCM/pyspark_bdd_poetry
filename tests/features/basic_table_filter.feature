Feature: Basic Table Filter Example

 Scenario: Simple filter on a table
  Given a spark session
  And a table called "students" containing
    | name:String | age:Int |
    | Fred        | 9       |
    | Mary        | 10      |
    | Bob         | 10      |
  When I select rows from "students" where "age" greater than "10" into table "results"
  Then the table "results" contains
    | name:String | age:Int |
    | Mary        | 10      |
    | Bob         | 10      |