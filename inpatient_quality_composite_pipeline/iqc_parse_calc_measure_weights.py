import helpers



measure_weight_df = helpers.parse_calculator_measure_weights()

print(measure_weight_df)
#measure_weight_df.to_csv('meas_weight_df.csv',index=False)
helpers.insert_measure_weights(measure_weight_df)
