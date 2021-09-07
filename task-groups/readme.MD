# Airflow Task Groups

Showcasing several ways to implement Airflow Task Groups.

## Description

Prior to the release of Airflow 2.0, the only way to group tasks and create modular workflows within Airflow was to use SubDAGs. Starting in Airflow 2.0, you can use Task Groups to organize tasks within your DAG's graph view in the Airflow UI. This avoids the added complexity and performance issues of SubDAGs, all while using less code!

## Getting Started


### Installing

In order to run these demos on your localhost, be sure to install:

* [Docker](https://www.docker.com/products/docker-desktop)

* [Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/resources/cli-reference)


### Executing demos

Clone this repository, then navigate to the ```task-groups``` directory and start your local Airflow instance:
```
astro dev start
```

In your browser, navigate to ```http://localhost:8080/```

* Username: ```admin```

* Password: ```admin```


### Notes about this repository's DAGs:

* apache_task_group_example - Apache's example DAG showcasing several features of Task Groups
* basic_task_groups - A basic DAG showing Task Groups
* dynamic_task_groups_independent - Creating dynamic Task Groups which do not depend on each other and can execute in parallel
* dynamic_task_groups_ordered - Creating dynamic Task Groups which have strict dependencies in place
* imported_task_groups - Importing Task Groups from a separate task_group_functions.py file to keep your DAG file clean and concise
* nested_task_groups - Nesting multiple Task Groups within one another and setting dependencies


## Additional Resources

* [Astronomer Guide - Using Task Groups in Airflow](https://www.astronomer.io/guides/task-groups)
* [Astronomer Airflow 2.0 Series - Task Groups](https://www.youtube.com/watch?v=e81DIxUmeYA)
* [Apache Airflow - Task Groups Documentation](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#taskgroups)

