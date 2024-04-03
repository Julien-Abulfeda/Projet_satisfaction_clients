import json


def clean_json_file(filename):
    """
    open the filename.json and clean the content
    """
    # Open the file in write mode, which automatically deletes all its content
    with open(filename, mode="w", encoding="utf-8") as json_file:
        json_file.write("[]")  # Écrire un objet vide dans le fichier


def add_data_to_json(filename, data):
    """
    Add data to the .json filename in parameter
    """
    # Load the existing JSON file
    with open(filename, "r") as json_file:
        json_content = json.load(json_file)
    # Add the new document to the existing data
    json_content.append(data)
    # Write the updated data in the JSON file
    with open(filename, mode="w", encoding="utf-8") as json_file:
        json.dump(json_content, json_file, indent=2)


def add_df_to_json(filename, df_data):
    """
    Add data to the .json filename in parameter
    """
    print("-------------------------> add_df_to_json")
    # Load the existing JSON file
    with open(filename, "r") as json_file:
        json_content = json.load(json_file)

    # print('json_content')
    # print(json_content)
    print(f"len(df_data): {len(df_data)}")
    # display(df_data)
    print(f"df_data.index: {df_data.index}")
    # Add the new documents to the existing data
    for i in range(len(df_data)):
        # print(f'dataframe {i}')
        # display(df_data.iloc[i, ])
        dict_comment = df_data.iloc[i,].to_dict()
        # print('dict')
        # print(dict_comment,'\n')
        json_content.append(dict_comment)

    # print('json_content')
    # print(json_content)
    # Write the updated data in the JSON file
    with open(filename, mode="w", encoding="utf-8") as json_file:
        json.dump(json_content, json_file, indent=2)


def get_data_from_json(filename):
    """
    open the filename.jason and clean the content
    """
    # Load the existing JSON file and return it
    with open(filename, "r") as json_file:
        json_content = json.load(json_file)
    return json_content
