# clean data to make less error in dataframe
# some feature engineering tasks will
import os
import pandas as pd
# delete all .xlsx files from files
def delete_exel_files(folder_path, extension):
    files = os.listdir(folder_path)

    # Iterate over the files and delete the ones with the given extension
    for file in files:
        if file.endswith(f'.{extension}'):
            file_path = os.path.join(folder_path, file)
            os.remove(file_path)
            
    print(f"All exel files are Deleted.")

# return year and month extracted from file path as integers
def get_year_month(file_path):
    base_filename = os.path.basename(file_path)
    # Split the base filename into name and extension
    filename, extension = os.path.splitext(base_filename)
    fragments = filename.split("_")
    return int(fragments[-2]), int(fragments[-1])

# merge all csv file from files
def merge_kba_csv_files(folder_path):
    files = os.listdir(folder_path)

    # Initialize an empty DataFrame to store the merged data
    merged_data = pd.DataFrame()
    # Iterate over the files and delete the ones with the .csv extension
    for file in files:
        if file.endswith(".csv") and file.startswith("kba"):
            # year, month = get_year_month(file)
            csv_file_path = os.path.join(folder_path, file)
            df = pd.read_csv(csv_file_path)
            merged_data = merged_data.append(df, ignore_index=True)
    
    # Save the merged data to a new CSV file
    merged_output_file = f"files/merged_data.csv"
    merged_data.to_csv(merged_output_file, index=False)
    if os.path.exists(merged_output_file):
        print(f"{merge_kba_csv_files} is created successfully.")

if __name__ == '__main__':
    delete_exel_files("files", "xlsx")
    merge_kba_csv_files("files")