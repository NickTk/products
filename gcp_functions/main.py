
from google.cloud import bigquery
import pandas as pd
import numpy as np

def check_similar_goods(request):
    """
    Calculates and returns cosine similarity of target id to a given list of candidate ids by their attributes 
    (price, good_group, manufactuter). If target id and at least one candidate id are present in orders,  
    also returns cosine similarity allowing for order attributes(failed & successfull orders, quantity) of goods
    besides price, good_group and manufactuter.
    
    Parameters:
    target_id (str): id of target product
    candidates (list): list of candidate ids
    
    Returns:
    candid_prods_str (str): string representation of dataframe containing cosine similarities of condidate products
    """
    
    request_json = request.get_json(silent=True)
    client = bigquery.Client()
    
    if request_json and 'id' in request_json:
        target_id = int(request_json['id'])
        candidates = request_json['cnd']
        all_products = (target_id, *candidates)
        cat_cols = ['goods_group', 'manufacturer']
        
        sql_prod = ('SELECT * FROM `thinking-seer-298510.client_data.products` '
                    f'WHERE product_id IN {all_products} ')
        
        sim_prods = client.query(sql_prod).to_dataframe()
        candid_prods = list_candidates(sim_prods, target_id, cat_cols)
        
        cols = 'product_id, status, quantity'
        sql_ord = (f'SELECT {cols} FROM `thinking-seer-298510.client_data.orders` '
                   f'WHERE product_id IN {all_products} ')
        
        sim_ord = client.query(sql_ord).to_dataframe()
        
        if len(sim_ord) < 1 and target_id not in sim_ord['product_id'].values:
            return candid_prods.to_string()
        else:
            sim_ord = add_order_attrs(sim_ord)
            sim_ord = pd.merge(left = sim_ord, right = sim_prods, on = 'product_id', how = 'left')

            candid_ords = list_candidates(sim_ord, target_id, cat_cols)
            candid_prods = pd.merge(left = candid_prods, right = candid_ords, 
                                    on = 'product_id', how = 'left', suffixes=('_prod', '_order'))
    
        candid_prods_str = candid_prods.to_string()
        
        return candid_prods_str

def get_cosine(target_arr, cnd):
    """ Returns cosine similarity of two vectors """
    return np.dot(target_arr, cnd) / (np.linalg.norm(target_arr) * np.linalg.norm(cnd))

def std_norm(df, col):
    """ Normalizes values of dataframe series using its standard deviation """
    return (df[col] - df[col].mean()) / df[col].std()

def list_candidates(df, target_id, cat_cols):
    """
    1. Generates dummy variables for categorical columns. 
    2. Normalizes product price values. 
    3. Performs a loop getting cosine similarities of candidate products
    """
    df = pd.get_dummies(df, columns = cat_cols)
    df['price'] = std_norm(df, 'price')
    df.set_index('product_id', inplace = True)

    target_arr = df.loc[target_id].to_numpy()
    candid_rows = df.drop(target_id)
    candid_arr = candid_rows.to_numpy()
    candid_rows['cosine'] = [get_cosine(target_arr, cnd) for cnd in candid_arr]
    
    return candid_rows.loc[:, 'cosine'].reset_index().copy()

def add_order_attrs(df):
    """
    Aggregates quantity, failed and successfull orders for target and candidate products
    """
    df = df.groupby(['product_id', 'status'], as_index = False).sum().\
    pivot_table(index = 'product_id', columns = 'status', aggfunc = 'sum', values = 'quantity').\
    fillna(0).reset_index()
    
    return df
