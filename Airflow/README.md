# Airflow Standalone

To install airflow and use Airflow without Docker in a virtual machine with limited resources, the following commands were used:

```sudo apt-get update```  
```sudo apt-get install python3-pip```  
```pip3 install apache-airflow```  
```pip3 install pandas```  
```pip3 install pymysql```  
```pip3 install boto3```  
```airflow standalone```  

This commands install python, airflow and the necesary libraries for the Dag file "data_cleaning.py", that can be found in dags\data_cleaning_dag

After this, Airflow webserver can be used by entering to "[Virtual-machine-ip]:8080" and the dag file can be run from there.