# Arvato Customer Segmentation Report

*Disclaimer: This project is my capstone project for the Data Science Nanodegree in Udacity and uses data provided by the institution. This also means that all the data used for the project is NOT publicly available*

# Introduction
This project is about building a customer segmentation for a mail-order company. There are two main questions involved in this challenge:  
1. Is it possible to identify demographic differences between general population and the customer base?  

Approach: Create an **unsupervised** customer segmentation to compare general population information to customer infomation.

2. Are we able to predict individuals more inclined to become customers of the company?

Approach: Create a **supervised** model to predict (possible) customers of the company.

To accomplish this task, 4 dataframes were made available:
- 2 containing general population demographic data and;
- 2 containing labeled entries for the supervised task.

# Main Results

- For the unsupervised phase, the results show that there are clusters that **enable us to see demographic difference between general and customer population**.
- For the supervised phase, the results compared to a baseline Logistic Regression model show good results. **This indicates that it is viable to predict probable customers**. The selected model was a Random Forest model that achieved approx. 0.73 of AUC-ROC score compared to a 0.62 baseline naïve Logistic Regression model.

# Running the project
To run the ETL and preprocessing steps, one can simply run the notebooks 00, 01 and 02. To run the models (notebooks 04, 05), preprocessing needs to be done with the data. This can be achieved by running the `preprocess.py` file on the data.

The `preprocess.py` file has an `-h` attribute on the command line to help with the parameters.

*Note: Running the project depends on having the actual data and respecting the file structure (data folder) built at the project.*  

All used notebooks are on the root folder of the repo

## Repo Structure
```
│   .gitignore
│   00_documentation_fix.ipynb -> Documentation fix for undocumented cols
│   01_etl.ipynb -> General demographic data ETL
│   02_demographic_preprocessing.ipynb -> Preprocessing tests
│   04_modelling_segments.ipynb -> Unsupervised learning model and results
│   05_mailout_predictions.ipynb -> Supervised learning model and results 
│   preprocess_demographics.py -> Preprocessing script
│   README.md
│
├───data -> Folders and subfolders to contain data and documentationfor use 
│   ├───raw -> Files in their raw format
│   │
│   ├───refined -> Final formatted data for use in modelling
│   |
│   └───trusted -> Intermediate files 
│
├───dump -> Deprecated files
│
├───models -> Models in PKL format
│
└───utils -> Folder for utility functions
```

# Acknowledgements and further info
The data and project are part of the [Udacity's Data Science Nanodegree](https://www.udacity.com/courses/all).  

There is an article associated with the findings and development process that can be found on my [Medium](https://medium.com/@luccagomes/youve-got-mail-machine-learning-for-customer-segmentation-2c90d9b9d58d).

For further information into the FAMD method used on the clustering stage, [check this article.](https://towardsdatascience.com/famd-how-to-generalize-pca-to-categorical-and-numerical-data-2ddbeb2b9210)