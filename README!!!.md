## Running pgAdmin in docker with local executor.
Here are the steps to take to get pgAdmin with docker on your machine. 
1. Clone this repo

2. Run this project requirements
```bash
python.exe -m pip install --upgrade pip
```
```bash
pip install --no-cache-dir -r requirements.txt
```

3. Install docker desktop application if you don't have docker running on your machine
- [Download Docker Desktop Application for Mac OS](https://hub.docker.com/editions/community/docker-ce-desktop-mac)
- [Download Docker Desktop Application for Windows](https://hub.docker.com/editions/community/docker-ce-desktop-windows)
4. Launch pgAdmin by docker-compose
```bash
docker-compose up -d
```
4.1 Check the running containers
```bash
docker ps
```
1. Open browser and type http://localhost:5050 to launch the PgAdmin ->
Username: pgadmin4@pgadmin.org (as a default)
Password: admin (as a default)

Port 5432
Username  default: postgres
Password  default postgres

![](images/screeenshot_pgAdmin.png)
