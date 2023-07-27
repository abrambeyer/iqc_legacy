import helpers


dump_df = helpers.parse_calc_excel_for_datadump('VIZIENT_CALC_PLACEHOLDER','2019 Q&A calculator Period 1')




helpers.insert_datadump_df(dump_df)

