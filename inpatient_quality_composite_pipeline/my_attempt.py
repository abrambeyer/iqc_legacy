def create_hyperlink_dict_wrangle(merged_df):
    merged_df = pd.DataFrame(merged_df,columns=['Cohort','Formal Name','Keyword/Metric','Domain'])
    merged_df = merged_df.drop_duplicates()
    merged_df['zipped_data'] = list(zip(merged_df.Cohort,merged_df['Domain'],merged_df['Formal Name'],merged_df['Keyword/Metric']))
    merged_df['zipped_keys'] = list(zip(merged_df.Cohort,merged_df['Keyword/Metric']))
    lookup_data_container = pd.Series(merged_df.zipped_data.values,index=merged_df.zipped_keys.values).to_dict()
    return(lookup_data_container)