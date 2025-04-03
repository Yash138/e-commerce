import pandas as pd
import re

def find_key_path(json_obj, target_key, path=None):
    """Recursively searches for a key in a nested JSON structure and returns the path to it."""
    if path is None:
        path = []

    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            new_path = path + [key]  # Extend path
            if key == target_key:
                return new_path  # Return the path when key is found
            result = find_key_path(value, target_key, new_path)
            if result:
                return result  # Return immediately once found

    elif isinstance(json_obj, list):
        for index, item in enumerate(json_obj):
            new_path = path + [index]  # Include list index in path
            result = find_key_path(item, target_key, new_path)
            if result:
                return result  # Return immediately once found

    return None  # Return None if the key is not found

def max_depth(d, depth=0):
    """
    Recursively finds the maximum depth of a nested dictionary.
    """
    if not isinstance(d, dict) or not d:
        return depth
    return max(max_depth(v, depth + 1) for v in d.values())

def extract_paths(data):
    
    def dfs(data, path=None):
        if path is None:
            path = []
        
        results = []
        
        if isinstance(data, dict) and 'category_id' in data:
            path.append(data['category_id'])
        
            subcategories = [v for k, v in data.items() if isinstance(v, dict) and 'category_id' in v]
        
            if subcategories:
                for subcategory in subcategories:
                    results.extend(dfs(subcategory, path[:]))
            else:
                results.append(path)
        
        return results
    output = []
    for key, value in data['bestsellers'].items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                output.extend(dfs(sub_value, [key]))

    # for i in output:
    #     i.remove('bestsellers')
    depth = max_depth(data['bestsellers'])
    df = pd.DataFrame(output, columns=['category']+['lvl'+str(i) for i in range(1, depth-1)])
    df['data_node'] = df.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    return df.to_dict(orient='records')

def extract_categories(data, results=None):
    if results is None:
        results = []
    def dfs(data):
        if isinstance(data, dict):
            # If category_id and category_name exist, add to results
            if "category_id" in data and "category_name" in data:
                results.append({
                    "category_id": data["category_id"],
                    "category_name": data["category_name"]
                })
            
            # Recursively explore subcategories
            for key, value in data.items():
                if isinstance(value, dict):  # Check for nested dictionaries
                    dfs(value)
        return results
    results = dfs(data)
    df = pd.DataFrame(results)    
    results = df.to_dict(orient='records')
    return results

def extract_numeric_part(value):
    if isinstance(value, str):
        # extract the numeric part
        x = [i for i in value.split(' ') if re.search(r'\d', i)]
        if len(x) > 1:
            raise Exception(f"Expected One Numeric Part, got {len(x)}:{x}")
        # remove special characters, exclude: decimal and alphanumeric letters
        if x:
            x = re.sub(r'[^a-zA-Z0-9\s\.]', '', x[0]).lower()
            if x[-1] == 'k':
                x = float(x.replace('k',''))*1000
            elif x[-1] == 'l':
                x = float(x.replace('l',''))*1_000_000
            else:
                x = float(x)
            return x
        return None
    
def safe_strip(value):
    return value.strip() if isinstance(value, str) else value

def safe_split(value, delimiter):
    return value.split(delimiter) if isinstance(value, str) else value

def safe_replace(value, old, new):
    return value.replace(old, new) if isinstance(value, str) else value

