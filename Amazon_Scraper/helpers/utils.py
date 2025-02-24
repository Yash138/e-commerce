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