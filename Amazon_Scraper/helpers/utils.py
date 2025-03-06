import pandas as pd

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
    for key, value in data.items():
        for sub_key, sub_value in value.items():
            output.extend(dfs(sub_value, [key]))

    for i in output:
        i.remove('bestsellers')
    df = pd.DataFrame(output, columns=['category']+['lvl'+str(i) for i in range(1, 21)])
    df['data_node'] = df.iloc[:, 1:22].ffill(axis=1).iloc[:, -1]
    return df.to_dict(orient='records')

def extract_categories(data, results=None):
    if results is None:
        results = []

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
                extract_categories(value, results)
    df = pd.DataFrame(results)
    results = df.to_dict(orient='records')
    return results