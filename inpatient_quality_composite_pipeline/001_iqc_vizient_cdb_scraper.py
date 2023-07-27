import helpers

# Run the core scraper function which does the following:

# step 1: Import all necessary functions from helpers.py.
# step 2: What period is this for?  Enter in the end date of for the Performance Close month.  Import
#         the time period data and generate a helper dictionary from it.
# step 3:  Import cohort data from the Vizient documentation file.  This gives the script a way to organize
#          the hospital data cleanly into their correct cohort.
# step 4:  Import the Vizient template hyperlink file and generate a helper dictionary from it.  This gives
#           the script a list of report links to iterate over.
# step 5:  Create main folder structure to store the files in.
# step 6:  Login to Vizient main page
# step 7:  Loop over hyperlink helper dictionary and time period helper dictionary for every
#          Vizient cohort and measure hyperlink, download the data, rename the file and store the excel file
# step 8:  While the script is looping over the hyperlink dictionary keys, it will determine how to run each
#          report template based on the cohort, measure name.  For example, EDAC, READM and PSI require nuanced
#          logic to generate correct values because their report templates are not exactly correct.
# step 9:  Once the report is downloaded to the Downloads folder, it will rename and move the file to the
#          designated data folder.  The file will be placed in its own folder named after the measure name abbreviation.
#          The file name should be similar to the following format: COHORT_MEASURE_PERIOD_TYPE.xlsx

remove_covid_pats = False

stats = helpers.core_scraper_function(remove_covid_pats)
print(stats)
print(str(stats[0]) + ' reports were downloaded.')
print('Elapsed time: ' + str(stats[1]))
