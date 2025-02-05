# Week 2

## no 1
a. 128.3

- set disabled to true keep the downloaded extracted file
id: purge_files
    type: io.kestra.plugin.core.storage.PurgeCurrentExecutionFiles
    description: This will remove output files. If you'd like to explore Kestra outputs, disable it.
    disabled: true
- choose year of 2020
- choose month of 12
- execute
- go to output -> extract -> ".csv" filename

## no 2
b. green_tripdata_2020-04.csv

variables:
  file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"

replace {{inputs.taxi}} with green
replace {{inputs.year}} with 2020
replace {{inputs.month}} with 04

## no 3
b. 24,648,499

- go to triger
- fill start date 01 january 2020
- fill end date 31 desember 2020
- select yellow taxi type
- execute
- in big query studio select dataset details - storage info - number of row

## no 4
c. 1,734,051

- go to trigger
- fill start date 01 january 2020
- fill end date 31 desember 2020
- select green taxi type
- execute
- select query tools
- type this query to show row count of the data
select count(*) from green_tripdata;

## no 5
d. 1,925,152
- go to trigger
- fill start date 01 march 2020
- fill end date 31 march 2020
- select yellow taxi type
- execute
- select query tools
- type this query to show row count of the data
select count(*) from yellow_tripdata;

*note: When I use local machine as a databse I uploud only a year because my memory can't handle it hehe, when a year is done, I delete it from database and I uploud other year.

## no 6
b. Add a timezone property set to America/New_York in the Schedule trigger configuration

reference: https://kestra.io/docs/workflow-components/triggers/schedule-trigger
