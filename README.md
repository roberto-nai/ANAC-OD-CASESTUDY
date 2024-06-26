# ANAC Open Data Case Study

![PyPI - Python Version](https://img.shields.io/badge/python-3.12-3776AB?logo=python)

The following script is based on a case study where ANAC Open Data are cross-referenced with ISTAT (istat_code) and Open BDAP (tax_code) data for some analysis on public tenders related to cleaning services (CPV division: 90).  

### > Directories

#### config
Configuration directory with ```config.yml```.  

#### open_data_anac
Directory with downloaded ANAC Open Data Catalogue (see this project: [https://github.com/roberto-nai/ANAC-OD-DOWNLOADER](https://github.com/roberto-nai/ANAC-OD-DOWNLOADER)).  
Open Data are also available on Zenodo: [https://doi.org/10.5281/zenodo.11452793](https://doi.org/10.5281/zenodo.11452793).  

#### open_data_bdap
Directory with data file from Open BDAP.

#### open_data_istat
Directory with data file from ISTAT.

#### plots
Directory with plots generated.   

#### stats
Directory with stats.

#### utility_manager
Directory with utilities functions.

### > Script Execution

#### ```01_anac_ca.ipynb```
Application to analyse the dataset.

#### ```conf_cols_excluded.json```
List of columns (features) to be ignored.

#### ```conf_cols_stats.json```
List of columns (features) on which perform stats.

#### ```conf_cols_type.json```
List of columns (features) with relative type (e.g. int, float, etc.).

### > Script Dependencies
See ```requirements.txt``` for the required libraries (```pip install -r requirements.txt```).  
