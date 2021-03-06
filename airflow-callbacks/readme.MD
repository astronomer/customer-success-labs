# Airflow Callbacks

Showcasing several ways to implement Airflow callbacks and notifications.

## Description

Monitoring tasks and DAGs at scale can be cumbersome. Sometimes you'd like to be notified of certain events, and not others. These DAGs cover several methods of implementing custom e-mail, Slack, and Microsoft Teams notifications so you can be confident you aren't missing critical events that may require immediate attention.

**Note**: The [MS Teams Hook](https://github.com/astronomer/customer-success-labs/tree/main/airflow-callbacks/include/hooks) and [MS Teams Operator](https://github.com/astronomer/customer-success-labs/tree/main/airflow-callbacks/include/operators) used in this repo were forked from [mendhak/Airflow-MS-Teams-Operator](https://github.com/mendhak/Airflow-MS-Teams-Operator)

## Slack Callback Examples
![Example Callbacks](https://github.com/astronomer/customer-success-labs/blob/main/airflow-callbacks/docs/images/notifications.png)

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


### Setting up Slack/Teams Connections
In order to receive callback notifications, you must also create your webhooks and set up your connections in the Airflow UI. follow the instructions found in the [Appendix section](https://docs.google.com/presentation/d/1lnu3IfM82I09yK7XuzGcroDNMlZpqs-3nARDCWpfaDI/edit#slide=id.ge7d1e4d78d_2_3) of the accompanying slide deck.


## Additional Resources

* [Notifications Overview Slides](https://docs.google.com/presentation/d/1lnu3IfM82I09yK7XuzGcroDNMlZpqs-3nARDCWpfaDI/edit?usp=sharing)
* [Astronomer Guide - Error Notifications in Airflow](https://www.astronomer.io/guides/error-notifications-in-airflow)
* [Astronomer Webinar - Monitor Your DAGs with Airflow Notifications](https://www.astronomer.io/events/webinars/dags-with-airflow-notifications/)
* [Configure Airflow Email Alerts on Astronomer](https://www.astronomer.io/docs/cloud/stable/customize-airflow/airflow-alerts#subscribe-to-task-level-alerts)
