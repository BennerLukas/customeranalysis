# Customer Analysis


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

It contains user data from the months october and november 2019.

## Usage
1. Download the data from [here](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store).
2. Put the data in ```/src/data``` and unzip it to csv.
3. make sure you have installed all modules from ```requirements.txt```.
4. Run the ```explore.ipynb``` for initial analysis (Attention the whole dataset is giant. For debugging use ```src/data/test_data.csv```)

## Usage K-Means
1. Execute cluster_customer.py
2. In case of an error mkdir ```log``` inside modelling folder (empty folder for log-files won't be synced with git)
3. Check if ```train, test, dev = customer.prep_data(False)``` in line 175 is False, so the data will be created from scratch.
4. Coffee break! This will take some time ;)
5. Rename the created csv (```data/customer_profile_new.csv```) file of the customer_profile inside the created folder and move it into ```src/data```
6. Next time use the exported csv instead of the raw data. Therefore, set line 175 to ```True``` again.

## Usage FPGrowth
File: ```/modelling/product_analysis/product_fpm.py```
1. To use the entire dataset set ``small_dataset=False`` in line 70
2. To filter the data on the event_type column one can set the filter_element in line 71 to be either purchases, carts, or views
3. The "focus" element in line 71 is the column that is used for the pattern mining. Useful columns to analyse are "product_id" and "category_code".
4. The parameters used in line 72 are directly used in the default [fpgrowth](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.ml.fpm.FPGrowth.html#pyspark.ml.fpm.FPGrowth) model from pyspark.


## About the project

### Team
Our Team:
- [Lukas Benner](https://github.com/BennerLukas)
- [Phillip Lange](https://github.com/Sabokou)
- [Alina Buss](https://github.com/Alinabuss)

### Project Target
Target of this project is to analyse customer behavior in online shops / e-commerce.

### Tools

#### Spark
Spark and Pyspark as the Python module is used as the "analytics engine" and modelling tool.

More about Spark see [here](https://spark.apache.org/).

#### Plotly
For easy and interactive plotting the library plotly was used.

The figures are saved in ```src/data/exports```.
More about Plotly see [here](https://plotly.com/).

## Documentation

Our more explicit documentation (in German) can be found [here](https://github.com/BennerLukas/customeranalysis/blob/main/documentation.md)


To find the interactive plots from plotly see ```src/data/exports``` and open the HTML-Files. 
For more Plots change the branch to "notebook_runned" to see the results on the whole dataset (runtime >3h).