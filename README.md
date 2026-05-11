# Airflow Tasks

<!-- public-repo-status -->
> Status: Legacy/reference for Airflow 1.10-era workflows. Issues and pull requests are disabled unless support is reopened.


This repository contains a set of tasks for Apache Airflow.
Prerequisites

    Python 3.6+
    Apache Airflow 1.10.15

# Installation

    Clone this repository to your local machine.
    Install the necessary Python libraries by running pip install -r requirements.txt.
    Copy the DAG files to your Airflow DAG folder (e.g., /usr/local/airflow/dags/).
    Configure the necessary connections and variables in your Airflow environment. 
    Refer to the Airflow documentation for more information.

# Usage

This repository contains the following DAGs:

   ## airflow_tasks: 
   This DAG creates a Hive table and writes information about the tasks in the DAG to the table.  
   It also contains the following checks:  
     
   check_dag_status:  
   This check runs every hour and sends an email notification if the DAG has unfinished tasks past 10:00 AM London time.  
   This check helps ensure that the DAGs are completing on time and alerts the appropriate team members if there are any issues.  
     
   check_task_start_time:  
   This check runs every hour and sends an email notification if a task is not starting within 5 minutes of the completion of the previous task.  
   This check helps ensure that the scheduler is working properly and that the tasks are running on schedule.  
     
   write_tasks_to_hive:  
   This operator writes the task information to a Hive table for historical purposes.  
   This information can be used for reporting or analysis.

This repository contains the following operators:

    SparkShellOperator: This operator runs a Spark command in a shell script.


# License

This project is licensed under the MIT License - see the LICENSE file for details.
Acknowledgements

This project was inspired by the Airflow documentation and the Airflow community. We thank them for their contributions and support.
## License

<!-- commercial-license-policy -->
This project is licensed for non-commercial use under the [PolyForm Noncommercial License 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0/).
Commercial use, resale, paid distribution, marketplace publication, SaaS hosting, or bundling into a paid product requires separate written permission from the author.
Project names, logos, package identifiers, store listings, screenshots, and other branding assets are not licensed for use in forks or redistributed builds.
