import pandas as pd


a = int(-9999)

df=pd.read_csv("/home/marcela/Downloads/RECOMMENDATIONFIX/recommendations_cleanup/test_9999.csv", quotechar='"');

print(df['multi_graph_id'])

df['multi_graph_id']=df['multi_graph_id'].fillna("-9999")

df['recommended_width']=df['recommended_width'].fillna("-9999")
print(df['multi_graph_id'])


print(df['recommended_width'])

df['output_size_id'] = df['output_size_id'].astype(str)

print(df['output_size_id'])

df.to_csv("/home/marcela/Downloads/RECOMMENDATIONFIX/recommendations_cleanup/test_9999_converted.csv", sep=',', encoding='utf-8',index=False, quotechar='"')
# data = [['Austria', 'Germany', 'hob', 'Australia'],
#         ['Spain', 'France', 'Italy', 'Mexico']]
#
# df = pd.DataFrame(data, columns = ['id','shop_id','method','ref_size_id','target_size_id','target_shop_shoe_id','external_id','info','is_recommendation_session','ip','created_at','updated_at','session_id','ref_shoe_id','output_size_numeric','output_size_id','output_scale','available','rounded','ref_size_numeric','recommended_size_numeric','precision','multi_graph_id','ref_shoe_scale','recommended_size_scale','recommended_width','request_id'])
#
# # Values to find and their replacements
# findL = ['NULL']
# replaceL = [-9999]
# #
# # # Select column (can be A,B,C,D)
# col = 'multi_graph_id';
#
# # Find and replace values in the selected column
# df[col] = df[col].replace(findL, replaceL)