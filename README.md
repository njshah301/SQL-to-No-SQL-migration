 
# Core Idea:
In this project, we have made an application which takes name and access path as input and can get mongoDB collections as an atlas in mongoDB compass. The algorithm can extract all the needed schema information such as fetch tables, primary key, referred table,reference table etc for given schema. After schema extraction, algorithm will embed or the bases of set of rules. After migration completed, collections are populated in mongoDB compass.

 ## Getting Started
Download [Docker Desktop](https://www.docker.com/products/docker-desktop) for Mac or Windows. [Docker Compose](https://docs.docker.com/compose) will be automatically installed. On Linux, make sure you have the latest version of [Compose](https://docs.docker.com/compose/install/).

Run in this directory to build and run the app:

```shell
docker-compose up
```
or if you want to run an image directly you can follow below commands:

#### To pull image:
```shell
docker pull njshah301/migration_from_sql_to_nosql:0.0.1
```

#### To build image: 
```shell
docker build -t njshah301/migration_from_sql_to_nosql:0.0.1 .
```

#### To run image:

```shell

docker container run -d -p  27010:27010 njshah301/migration_from_sql_to_nosql:0.0.1

```


The `Migration` app will be running at [http://localhost:27010](http://localhost:27010).

## Requirements: 
#### 1>  PostGreSQL 
#### 2> MongoDB Compass
 
## How it Works ?

### Example Database  [BasketBall Training Manangement System] 
![image](https://user-images.githubusercontent.com/58663029/176962556-5141254e-7265-4717-b614-725eda6a5b98.png)


#### We have 17 table in SQL Schema.

### Schema Diagram

![image](https://user-images.githubusercontent.com/58663029/176962667-bf4f4f28-8a2c-4e9c-ad99-97ab417d7bb0.png)


### HomePage of the Application:

![image](https://user-images.githubusercontent.com/58663029/176962810-8b156f9f-a781-4ca4-8c93-303b9019dffe.png)


## MongoDB Compass View:
![image](https://user-images.githubusercontent.com/58663029/176962890-6f49f266-920c-4c60-aea3-baffdbc999b2.png)


### Input Parameters:
 
![image](https://github.com/njshah301/SQL-to-No-SQL-migration/assets/58663029/56f5dc54-8056-4e8c-9ab7-6e577be428b3)
 
 
 #### Note: Access path is a text file, which is given to the input to prioritized some collections which are frequently queried out. The migration Algorithm takes care about these acess paths.
 
### Example of Acesss path for our BasketBall Training Management System:

![image](https://user-images.githubusercontent.com/58663029/176963591-0eb75e2a-2b98-4fda-84cc-0525906e03c7.png)



### Processing of the Algorithm

![image](https://user-images.githubusercontent.com/58663029/176963662-388f91c6-1191-4bc8-bbb1-bfb83cc7b63d.png)



### Migration Done Sucessfully

![image](https://user-images.githubusercontent.com/58663029/176963697-7d83bb4d-3764-4896-af84-ec8cb4a25a53.png)

Now, if we see in the MongoDB compass, Database (Sports_Training_Demo) is created automatically and migration has been done.


## Results:

### Database Created Sucessfully

![image](https://user-images.githubusercontent.com/58663029/176963965-0871c651-68fc-4313-9301-a5403eebb73a.png)

### Migration has been Done

![image](https://user-images.githubusercontent.com/58663029/176964045-6b10cb17-8542-4a96-a336-5dde3d54435d.png)

#### Note: As you can see that, in SQL we have 17 tables and in MongoDB we got 7 collections. Thus, Denormalization has been performed.

### Player Collection View

![image](https://user-images.githubusercontent.com/58663029/176964225-4f98968c-db0f-4113-8838-2604c7c0b599.png)


#### This project was done as the Summer Research Internship.


