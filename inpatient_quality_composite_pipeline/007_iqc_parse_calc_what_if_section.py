import helpers
#VIZIENT_CALC_PLACEHOLDER
#NM_FSCL_YTD
time_period_type = 'VIZIENT_CALC_PLACEHOLDER'

time_period_end_datetime = '1900-01-01 00:00:00.0000000'

what_if_result_df = helpers.parse_calculator_what_if_section(time_period_type,time_period_end_datetime)

what_if_result_df = what_if_result_df[~what_if_result_df['measure_value'].isna()]

#print(what_if_result_df)
#what_if_result_df.to_csv('test_wi.csv')

helpers.insert_what_if_section(what_if_result_df)

#For hospital #356 (VALLEY_WEST_COMMUNITY_HOSPITAL) measure_id = 183 (Colonoscopy Revisits within 7-days) is not getting picked up
###Resolution: write the dataframe to a csv--> alter csv --> write the csv back to a dataframe then insert

#WRITE TO CSV
#what_if_result_df.to_csv(r'P:\Datastore02\Analytics\20 Current Staff\Mike Becker\test_wi.csv')
###--> Then add 183 to missing measure_id, AM

#READ IN THE ADJUSTED CSV INTO A DATAFRAME THEN INSERT 
#import pandas as pd
#fixed_df = pd.read_csv (r'P:\Datastore02\Analytics\20 Current Staff\Mike Becker\test_wi_183.csv')
#fixed_df.to_csv(r'P:\Datastore02\Analytics\20 Current Staff\Mike Becker\fixed_183.csv')
#helpers.insert_what_if_section(fixed_df)