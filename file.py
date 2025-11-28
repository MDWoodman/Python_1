def save_array_to_file(array, filename):
    with open(filename, 'w') as file:
        for item in array:
            file.write(f"{item}\n")
#generate function save jsonstring to file

def save_json_to_file(json_string, filename):
    with open(filename, 'w') as file:
        file.write(json_string)