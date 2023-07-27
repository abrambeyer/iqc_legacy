import helpers

domain_weight_df = helpers.parse_calculator_domain_weights()

print(domain_weight_df)
helpers.insert_domain_weights(domain_weight_df)
