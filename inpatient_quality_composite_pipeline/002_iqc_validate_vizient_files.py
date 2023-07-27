import helpers


# This function validates all the downloaded Vizient Q&A files the resulted from the vizient_qa_scraper.py file.
# This function will open all Vizient Q&A files within a folder structure and
# check certain filter parameters against the correct configurations defined in a file called vizient_qa_validation.xlsx.

# Function to validate the downloaded Vizient Q&A files.
# Dependencies:
#   This file requires an excel file called validation_file/vizient_qa_validation.xlsx
#   Inside this file, there should be the following two columns called 'validation_item' and 'value'.
#   Format should be similar to as below:
#validation_item	                    value
#CAMC/LSCCMC Risk Adjustment Model	    2018 Risk Model (AMC)
#CCMC Risk Adjustment Model	            2018 Risk Model (Community)
#CAMC/LSCCMC AHRQ Version	            8.0 (CMS Safety)
#CCMC AHRQ Version	                    8.0 (CMS Safety)
#Standard Time Period	                Aug 2018 to Apr 2019
#THK Time Period	                    Jul 2018 to Mar 2019
#READM/EDAC Time Period	2019            Quarter 2,2019 Quarter 1,2018 Quarter 4,2018 Quarter 3
#Focus Hospital	                        NORTHWESTERN_MEMORIAL 140281

# Output:
#  This function will output a file validation_results.csv in the same folder as the validation_file.
#  The output file will have one row for every Vizient Q&A data file and indicate whether it has passed or failed validation.


# Set up:
# 1.  Run vizient_qa_scraper.py to download the Vizient Q&A files
# 2.  Update vizient_qa_validation.xlsx to indicate what the correct risk model, ahrq model, time period and focus hospital
#     should be for the downloaded files.

# run the validation script.
#helpers.validate_downloaded_files()

#UL006
remove_covid_pats = False
helpers.validate_downloaded_files(remove_covid_pats)