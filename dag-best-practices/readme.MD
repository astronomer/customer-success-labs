# DAG Writing Best Practices



## Description

Because Airflow is 100% code, knowing the basics of Python is all it takes to get started writing DAGs. However, writing DAGs that are efficient, secure, and scalable requires some Airflow-specific finesse. In this respository, we'll cover some best practices for developing DAGs that make the most of what Airflow has to offer.

## Getting Started


Head over to the repository associated with the best-practices webinar linked in the Additional Resources section below:  

## [webinar-dag-writing-best-practices](https://github.com/astronomer/webinar-dag-writing-best-practices)

### Installing

In order to run these demos on your localhost, be sure to install:

* [Docker](https://www.docker.com/products/docker-desktop)

* [Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/resources/cli-reference)


### Executing demos

Clone this repository, then navigate to the ```webinar-dag-writing-best-practices``` directory and start your local Airflow instance:
```
astro dev start
```

In your browser, navigate to ```http://localhost:8080/```

* Username: ```admin```

* Password: ```admin```


## Additional Resources

* [DAG Writing Best Practices in Apache Airflow - Astronomer Guide](https://www.astronomer.io/guides/dag-best-practices)
* [DAG Writing Best Practices in Apache Airflow - Astronomer Webinar](https://www.astronomer.io/events/webinars/dag-writing-best-practices-in-airflow/)
* [Templating and Macros in Airflow - Astronomer Guide](https://www.astronomer.io/guides/templating)
