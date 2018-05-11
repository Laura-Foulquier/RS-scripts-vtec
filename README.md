# Build SQL Scripts VTEC



## What does it do
Simple python scripts which creates the SQL scripts needed to load the data from S3 to Redshift
Once created, import the SQL script to Redshift and just run the different commands.


## Which version of Python is this developed in
This script has been developed using Python 2.7

## Which libraries do I need to install
You will need to install the following addtional libraries:

* pandas

## What additional information do I need to provide 

* The VTEC period you are interested in, ie : 1713, 1714 ...
* The start date of the period in format: YYYY/MM/DD
* The end date of the period in format: YYYY/MM/DD
* Your AWS ACCESS KEY ID
* Your AWS SECRET ACCESS KEY
* If the uners are from the same source (ie some data were reprocessed from June to November included 2016, and are saved in s3://tef.prod.uk/core/ukvtec5/cip/common/uner/)

## Where is the script saved

The script will be saved as <VTEC period>.sql in ./loading_scripts.


## What are the loaded data 

* cell_reference from s3://tef.prod.uk/core/5/cip/common/extended_ucr/
* cell_range from s3://tef.prod.uk/core/5/cip/common/daily_cell_record/
* uner from s3://tef.prod.uk/core/5/cip/common/uner/


All tables will be loaded in the following playpen: *playpen_vtec*





