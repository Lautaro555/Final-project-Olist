<h1 align=center> FINAL PROJECT </h1>
<p align="center"> <img alt="DASS" src="https://user-images.githubusercontent.com/110403753/215381380-d2a74ac1-7ae4-41a9-90c8-45c26a813f57.jpeg" height=200px> </p>
<h2 align=center> Olist E-commerce complete solution </h2>
<br>
¡Hello! We are P. Castro, G. Fernandez, L. Pierotti and F. Tacchela and this is our final product for the Olist company, which is the last part of the training for the Henry Data Science bootcamp.
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
* <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" height=20px> [Python](https://docs.python.org/3/)
    * <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/pandas/pandas-original.svg" height=20px> [Pandas](https://pandas.pydata.org/)
    * <img src="https://user-images.githubusercontent.com/110403753/215380299-c2e13fce-1063-4626-9af7-fd708af21aed.svg" height=20px> [sklearn](https://scikit-learn.org/stable/index.html)
    * <img src="https://user-images.githubusercontent.com/110403753/215380497-fee65027-e34d-4a3d-8bad-3da819e2538f.svg" height=20px> [Matplotlib](https://matplotlib.org)
    * [Seaborn](https://seaborn.pydata.org)
    * [SqlAlchemy](https://www.sqlalchemy.org/)
    * [PyMysql](https://pypi.org/project/PyMySQL/)
    * [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
* <img src="https://user-images.githubusercontent.com/110403753/215379351-dcfd79cd-a105-4021-9f97-15ce17f750ce.svg" height=20px> [AWS](https://aws.amazon.com/es/)
    * <img src="https://user-images.githubusercontent.com/110403753/215379089-297eca2c-319e-49ee-ac8b-22a4f357ee20.svg" height=20px> [EC2](https://aws.amazon.com/es/ec2/?nc2=h_ql_prod_fs_ec2) - [Lightsail](https://aws.amazon.com/es/lightsail/?nc2=h_ql_prod_fs_ls)
    * <img src="https://user-images.githubusercontent.com/110403753/215378849-e0127a90-1638-4f7c-8ac1-e80bf09cf769.svg" height=20px> [Simple Storage Service (S3)](https://aws.amazon.com/es/s3/?nc2=h_ql_prod_fs_s3)
    * <img src="https://user-images.githubusercontent.com/110403753/215379152-e23020e5-df92-43c9-81d9-cdce6764f635.svg" height=20px> [RDS](https://aws.amazon.com/es/rds/?nc2=h_ql_prod_fs_rds)
    * <img src="https://user-images.githubusercontent.com/110403753/215379572-7a77a741-ce6e-4f34-b500-9307ed662cd2.svg" height=20px> [CloudWatch](https://aws.amazon.com/es/cloudwatch/?nc2=type_a)
    * [IAM](https://aws.amazon.com/es/iam/?nc2=type_a)
* <img src="https://user-images.githubusercontent.com/110403753/215380649-1714e39d-a307-4d2d-a304-93d16eb56863.svg" height=20px> [Airflow](https://airflow.apache.org/)
* <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/mysql/mysql-original.svg" height=20px> [MySQL](https://www.mysql.com/)
* <img src="https://user-images.githubusercontent.com/110403753/215380876-f58cce4b-dd02-4fe5-bfe3-0994f0f4c34d.svg" height=20px> [PowerBI](https://powerbi.microsoft.com/es-es/)
* <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/fastapi/fastapi-original.svg" height=20px> [FastAPI](https://fastapi.tiangolo.com/)
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
9. Extras: Recommendations to the Olist clients

### Repository archives
- [**API**:](./API/) Inside this folder is the script for the fastAPI run by Uvicorn on Lightsail VM.
- [**Airflow**:](./Airflow/) Inside this folder is the script for the Pipeline run by Airflow on Lightsale VM.
- [**Dashboards**:](./Dashboards/) Inside this folder is the PowerBI file with the final Dashboard.
- [**Datasets**:](./Datasets/) Inside this folder are the raw files consulted to carry out the project, and also the files created to extract relevant data.
- [**Notebooks**:](./Notebooks/) Inside this folder are the Jupyter notebook files with all the different tasks made. There is also a folder in which the ML Pipeline is saved, to avoid running all the training sessions again.
- [**Recommendations**:](./Recommendations/) Inside this folder is the PowerBI file with our suggestions to the company, and a PDF that can be emailed to the clients.
- [**Report**](./Report_english.pdf/): Complete details of all the steps and the details of the proyect workplan, in spanish and english version.

<hr>

Here is our contact info:  
<a href="https://www.linkedin.com/in/pablo-castro-/"><img alt="Pablo" title="Connect with Pablo" src="https://img.shields.io/badge/P.Castro-0077B5?style=flat&logo=Linkedin&logoColor=white"></a>
<a href="https://www.linkedin.com/in/fernandezguillermo"><img alt="Guillermo" title="Connect with Guillermo" src="https://img.shields.io/badge/G.Fernandez-0077B5?style=flat&logo=Linkedin&logoColor=white"></a> 
<a href="https://www.linkedin.com/in/lautaro-pierotti/"><img alt="Lautaro" title="Connect with Lautaro" src="https://img.shields.io/badge/L.Pierotti-0077B5?style=flat&logo=Linkedin&logoColor=white"></a> 
<a href="https://www.linkedin.com/in/franco-tacchella/"><img alt="Franco" title="Connect with Franco" src="https://img.shields.io/badge/F.Tacchella-0077B5?style=flat&logo=Linkedin&logoColor=white"></a>   

Thank you very much for reading all!