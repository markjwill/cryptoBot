import json

# Sample list of keys and comma-separated values string
keys = ["key1", "key2", "key3"]
values_string = "value1,value2,value3"

# Convert the comma-separated string to a list of values
values = values_string.split(',')

# Combine keys and values into a dictionary
data_dict = dict(zip(keys, values))

# Convert the dictionary to a JSON object
json_object = json.dumps(data_dict, indent=4)

# Print the JSON object
print(json_object)
