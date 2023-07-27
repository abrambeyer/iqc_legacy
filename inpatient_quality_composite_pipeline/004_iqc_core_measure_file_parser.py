import helpers


#test_dfs = helpers.vizient_data_folder_walker()

# parse the vizient q&a reports to get measure values
final_df = helpers.core_measure_data_folder_walker_and_prep_for_db_inserts()


calc_nm_input = '2020 Q&A calculator Period 2'
period_type_input = 'NM_FSCL_YTD'
period_end_dts_input = '2021-08-31 23:59:59'

# convert the result of core_measure_data_folder_walker_and_prep_for_db_inserts() function
# into form ready to insert into database
insert_ready_df = helpers.convert_metric_df_to_insert_measure_value_df(final_df,calc_nm_input,period_type_input,period_end_dts_input)

insert_ready_df = insert_ready_df.drop_duplicates()

print(insert_ready_df)


insert_ready_df['measure_value'] = insert_ready_df['measure_value'].astype('float')

# insert dataframe into database
#helpers.insert_measure_values_from_reports_df(insert_ready_df)

