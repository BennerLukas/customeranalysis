# customeranalysis
Analyse customer behavior in online shops / e-commerce

For data storage a Postgresql Database will be used which is first initialised under docker with:
```bash
docker container run -p 5433:5432 --name data_exploration -e POSTGRES_PASSWORD=1234 postgres:13.2
```