import helpers


#choose hospital rank type.  Choose either 'Possible Rank' or 'Target Ranking: '
hospital_rank_type = 'Possible Rank'

#VIZIENT_CALC_PLACEHOLDER
#NM_FSCL_YTD
time_period_type = 'NM_FSCL_YTD'

time_period_end_datetime = '06/30/2023 23:59:59'

result_df = helpers.parse_calculator_hosp_ranks(hospital_rank_type,time_period_type,time_period_end_datetime)

print(result_df)



#result_df.to_csv(r'S:\Datastore02\Analytics\200 NM Performance\Analytics Requests\iqc_dec_mortality_without_covid_pats\dec_orig_ranks_no_covid_pats.csv')

helpers.insert_hospital_ranks(result_df)
