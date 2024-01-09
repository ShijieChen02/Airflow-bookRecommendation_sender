# Airflow-bookRecommendation_sender

## 1. background and functions
This airflow dag will programmatically fetch the latest list of Combined Print & E-Book Fiction and Combined Print & E-Book Nonfiction through NYTimes Books API and send the results to some email recipients. Detailed expected behaviors are listed below.
1. NyTimes Best Sellers list is updated once every week. The dag should be designed in a way that it will just pull the list of a given week for no more than once. But the dag should still be scheduled as a daily job.
2. Define a list of default email recipients who will receive an email every time the weekly Best Sellers list gets updated. But a recipient should not receive repeat emails for the list of the same week. You can decide whether this task should be under the same dag that pulls the Best Sellers list or it should be handled by a separate dag.
3. An admin can also manually trigger the dag so additional recipients can receive the latest list by passing their email addresses through airflow dag configuration json.


## 2. Clone this repo and create 'logs/' ， 'plugins/' and 'config/' directories as Airflow requirs in official documentation.
![image](https://github.com/ShijieChen02/Airflow-bookRecommendation_sender/assets/147095965/7b3ab617-1901-4b16-a758-f3a099c0fee3)

## 3. Use Airflow on Docker: docker compose up -d to Start the services in containers

## 4. 8080 port is the webserver of the Airflow
![image](https://github.com/ShijieChen02/Airflow-bookRecommendation_sender/assets/147095965/0dfc68c1-af99-43b8-9181-f2f15e8bef5b)


## 5. you could add your new appened receivers after 'trigger' the dag in 'configuration json'.
format:  {"email": ["example1@domain.com","example1@domain.com"]}
![image](https://github.com/ShijieChen02/Airflow-bookRecommendation_sender/assets/147095965/d4c169b2-c2a3-4c3e-a17f-c136f0ef02a3)

## 6. If some problem happens
1. check the log in webserver.
2. If it's the from_email's problem, contact me through schen935@wisc.edu
