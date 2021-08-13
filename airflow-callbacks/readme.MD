# Airflow Callbacks

Showcasing several ways to implement Airflow callbacks and notifications.

## Description

Monitoring tasks and DAGs at scale can be cumbersome. Sometimes you'd like to be notified of certain events, and not others. These DAGs cover several methods of implementing custom e-mail, Slack, and Microsoft Teams notifications so you can be confident you aren't missing critical events that may require immediate attention.

## Getting Started

### Dependencies

To implement notifications via Slack, add this to your requirements.txt:
```
apache-airflow-providers-slack
```
To implement notifications via Slack or Microsoft Teams, add this to your requirements.txt:
```
apache-airflow-providers-http
```

### Installing

In order to run these demos on your localhost, be sure to install:

* [Docker](https://www.docker.com/products/docker-desktop)

* [Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/resources/cli-reference)


### Executing demos

Clone this repository, then navigate to the ```airflow-callbacks``` directory and start your local Airflow instance:
```
astro dev start
```

In your browser, navigate to ```http://localhost:8080/```

* Username: ```admin```

* Password: ```admin```


## Additional Resources

* [Notifications Overview Slides](https://docs.google.com/presentation/d/1lnu3IfM82I09yK7XuzGcroDNMlZpqs-3nARDCWpfaDI/edit?usp=sharing)
* [Astronomer Guide - Error Notifications in Airflow](https://www.astronomer.io/guides/error-notifications-in-airflow)
* [Astronomer Webinar - Monitor Your DAGs with Airflow Notifications](https://www.astronomer.io/events/webinars/dags-with-airflow-notifications/)
* [Configure Airflow Email Alerts on Astronomer](https://www.astronomer.io/docs/cloud/stable/customize-airflow/airflow-alerts#subscribe-to-task-level-alerts)
