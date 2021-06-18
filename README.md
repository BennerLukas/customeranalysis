# Customer Analysis

Target of this project is to analyse customer behavior in online shops / e-commerce.

[![GitHub issues](https://img.shields.io/github/issues/BennerLukas/customeranalysis)](https://github.com/BennerLukas/customeranalysis/issues)
[![GitHub forks](https://img.shields.io/github/forks/BennerLukas/customeranalysis)](https://github.com/BennerLukas/customeranalysis/network)
[![GitHub stars](https://img.shields.io/github/stars/BennerLukas/customeranalysis)](https://github.com/BennerLukas/customeranalysis/stargazers)
[![GitHub license](https://img.shields.io/github/license/BennerLukas/customeranalysis)](https://github.com/BennerLukas/customeranalysis/blob/main/LICENSE)

![Python](https://img.shields.io/badge/Language-Python-lightgrey?style=flat&logo=python)
![Jupyter](https://img.shields.io/badge/Tool-Jupyter-lightgrey?style=flat&logo=jupyter)
![Spark](https://img.shields.io/badge/Library-Spark-lightgrey?style=flat&logo=Apache-Spark)
![Plotly](https://img.shields.io/badge/Library-Plotly-lightgrey?style=flat&logo=Plotly)


## Dataset
The dataset is from [kaggle.com](kaggle.com) and can be found [here](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store).

It contains of user data from the months October and November 2019.

## Usage
For data storage a Postgresql Database can be used which is first initialised under docker with:
```bash
docker container run -p 5433:5432 --name data_exploration -e POSTGRES_PASSWORD=1234 postgres:13.2
```

## About the project

### Team

- [Lukas Benner](https://github.com/BennerLukas)
- [Phillip Lange](https://github.com/Sabokou)
- [Alina Buss](https://github.com/Alinabuss)

### Project Goal

### Tools

## Further documentation

see [documentation.md](documentation.md)