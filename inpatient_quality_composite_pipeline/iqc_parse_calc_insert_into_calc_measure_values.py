import helpers

time_period_type = 'VIZIENT_CALC_PLACEHOLDER'

time_period_end_datetime = '1900-01-01 00:00:00.0000000'

measures_and_links_df = helpers.parse_calculator_measures_and_links(time_period_type,time_period_end_datetime)


print(measures_and_links_df)
measures_and_links_df.to_csv('m_and_l_df.csv',index=False)

measures_and_links_df = measures_and_links_df[~measures_and_links_df['measure_id'].isna()]
#remove rows without measure_id join success.  CAUTION:  Be sure to check your dataframe to make sure this is logical.
#As of July, 2023, I did this because there were several new Experimental measures which are not used but exist 
#in the calculators/hyperlink files without urls.  As of July, 2023, these are not "real" measures so I did not add them 
#to the measures table and will exclude them for the purposes of FY24.  In the future, these measures and others may be 
#truly used in the Vizient Q&A and, therefore, should be supported and added to the measure table.

#In July, 2023, I confirmed the only measures missing a measure_id (failing left join) were "MISSING RACE OR ETHNICITY" measures.  As of July, 2023, 
#these were experimental-only measures and not used in the baseline Vizient Q&A.  Therefore, this is acceptable to remove.

helpers.insert_into_calc_measure_values_tb(measures_and_links_df)