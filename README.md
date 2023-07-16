# AeroDe


**STEPS**:
1. *Create "airflow" directory and download docker-compose.yaml into it. (**make sure internal and extrenal ports are mapped for Postgres container**, otherwise you won't be able to connect dbeaver or another database IDE for that matter to you PostgreSQL database)*

2. *Create sub-directories such as  "./logs", "./dags", "./config" and "./plugins"*

3. *Run command ```docker compose up -d ```  inside  "airflow" folder and wait until run is success.*

4. *Download and put  ETL_RUN.py to your "./dags" folder* 

5.  Use Dbeaver to create connection (username and password are "airflow")
![](https://i.ibb.co/SwsB9HQ/image.jpg)

6. Use DDL's from SQL folder to create necessary entities. 

7. Go to  http://localhost:8080/ ,find your dag and run it.
![](https://i.ibb.co/hVs7SPj/image.jpg)

8. Check result in the tables below:
   
![](https://i.ibb.co/4gP1Z7W/image.jpg)
