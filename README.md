<h1 align=center> FINAL PROJECT </h1>
<h2 align=center> Olist E-commerce complete solution </h2>

<br>
¡Hello! We are P.Castro, G.Fernandez, L.Pierotti and F.Tacchela and this our final product for the Olist company, which is last part of the training for the Henry Data Science bootcamp.
The entire proyect is in English, as requested by the Product Owner.

<hr>

## Objetive
With the primary objective of continuing to connect small businesses with larger markets and improving the user experience, the goal is to find innovative solutions that allow its users to sell their products to a greater number of customers. The given data is from 2016 to 2018.

[Project description (in Spanish)](https://github.com/soyHenry/PF_DS/blob/main/Proyectos/E-Commerce.md)

### Context
In 2021, the retail sale of products through e-commerce meant an approximate balance of 5.2 trillion dollars worldwide and it is inferred that this figure will increase by 56% in the coming years, reaching 8.1 trillion in 2026.
Olist is a Brazilian company that provides e-commerce services for SMEs that works as a marketplace, that is, it works as a "store of stores" where different sellers can offer their products to final consumers.

<p align="center"> <img alt="Olist" src="https://th.bing.com/th/id/R.780cecffbdba874cf5eb53caef3394e4?rik=BLH2EB5kU13mEg&pid=ImgRaw&r=0" height=200px> <img alt="Olist2" src="https://i1.wp.com/blog.olist.com/wp-content/uploads/2015/02/post-olist-p.png?fit=500%2C400&ssl=1" height=200px></p>

### Tech stack
* [Python](https://docs.python.org/3/)
    * [Pandas](https://pandas.pydata.org/)
    * [Numpy](https://numpy.org)
    * [sklearn](https://scikit-learn.org/stable/index.html)
    * [scipy](https://scipy.org)
    * [joblib](https://joblib.readthedocs.io/en/latest/)
    * [Seaborn](https://seaborn.pydata.org)
    * [Matplotlib](https://matplotlib.org)
    * [SqlAlchemy](https://www.sqlalchemy.org/)
    * [PyMysql](https://pypi.org/project/PyMySQL/)
    * [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
* [AWS](https://aws.amazon.com/es/)
    * Lightsail
    * S3
    * RDS
    * IAM
* [Airflow](https://airflow.apache.org/)
* [MySQL](https://www.mysql.com/)
* [PowerBI](https://powerbi.microsoft.com/es-es/)
* [FastAPI](https://fastapi.tiangolo.com/)
* [Uvicorn](https://www.uvicorn.org/)

<hr>

## Workplan:
1. EDA and Data quality analysis 
2. Product, objectives and KPIs
3. DataLake and ER Model
4. Data Pipeline
5. Automation with incremental values
6. Database and Dashboard connection
7. Extras: Machine Learning Model
8. Extras: API to consult KPIs

### Repository archives
- [**API**:](./API/) Inside this folder is the script for the fastAPI run by Uvicorn on Lightsail VM.
- [**Airflow**:](./Airflow/) Inside this folder is the script for the Pipeline run by Airflow on Lightsale VM.
- [**Datasets**:](./Datasets/) Inside this folder are the raw files consulted to carry out the project, and also the files created to extract relevant data.
- [**Notebooks**:](./Notebooks/) Inside this folder are the Jupyter notebook files with all the different tasks made. There is also a folder in which the ML Pipeline is saved, to avoid running all the training sessions again.
- [**Dashboard**:](./Dashboard.pbix) Final file for the Dashboard.
- [**Report**:](./Report.pdf) Complete details of all the steps and the details of the proyect workplan.

<hr>

Here is our contact info:  
<a href="https://www.linkedin.com/in/pablo-castro-/"><img alt="Pablo" title="Connect with Pablo" src="https://img.shields.io/badge/P.Castro-0077B5?style=flat&logo=Linkedin&logoColor=white"></a>
<a href="https://www.linkedin.com/in/fernandezguillermo"><img alt="Guillermo" title="Connect with Guillermo" src="https://img.shields.io/badge/G.Fernandez-0077B5?style=flat&logo=Linkedin&logoColor=white"></a> 
<a href="https://www.linkedin.com/in/lautaro-pierotti/"><img alt="Lautaro" title="Connect with Lautaro" src="https://img.shields.io/badge/L.Pierotti-0077B5?style=flat&logo=Linkedin&logoColor=white"></a> 
<a href="https://www.linkedin.com/in/franco-tacchella/"><img alt="Franco" title="Connect with Franco" src="https://img.shields.io/badge/F.Tacchella-0077B5?style=flat&logo=Linkedin&logoColor=white"></a>   

Thank you very much for reading all!