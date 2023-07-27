
import helpers

current_cohort_calc = 15	#2022 Q&A calculator Period 3


#parse the vizient q&a reports to get measure values
final_df = helpers.vizient_data_folder_walker_and_prep_for_db_inserts(current_cohort_calc)
#final_df.to_csv(r"P:\Datastore02\Analytics\230 Inpatient Quality Composite\data\file_parsing_output\final_df.csv",index=False)
#print(final_df)

calc_nm_input = '2022 Q&A calculator Period 3'
period_type_input = 'NM_FSCL_YTD'
period_end_dts_input = '2023-06-30 23:59:59'

#convert the resulting dataframe from the above function and convert it into the correct format to be
#accepted by the database table vizient_qa.measure_values.

insert_ready_df = helpers.convert_metric_df_to_insert_measure_value_df(final_df,calc_nm_input,period_type_input,period_end_dts_input)

insert_ready_df.drop_duplicates(subset = ['calc_id','hospital_id','measure_id','period_id','measure_value_id'],keep='first',inplace=True)


print(insert_ready_df.shape)
#print(insert_ready_df)
insert_ready_df.to_csv(r"P:\Datastore02\Analytics\230 Inpatient Quality Composite\data\file_parsing_output\insert_ready_df.csv",index=False)


#insert the data into measure_values table of the database.


helpers.insert_measure_values_from_reports_df(insert_ready_df)

