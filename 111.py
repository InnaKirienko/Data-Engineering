from sales_api import get_sales_data

data ={"date": "2022-08-09",
"raw_dir": "C:\\Users\\user\\Documents\\Data-Engeniring\\file_storage\\raw\\sales\\2022-08-09"}

    # Checking whether the object and its properties exist
date = data['date']
raw_dir = data['raw_dir']

        # Creating a storage path if none exists
os.makedirs(raw_dir, exist_ok=True)

        # Cleaning the contents of the directory
clear_directory(raw_dir)

        # Receiving data from the API and uploading them to a file in the directory specified by the request
page = 1

while page > 0:  # Condition to stop when "error" is encountered
    sales_data = get_sales_data(date, page)
    if "error" in sales_data:
        page = 0  # Set page to 0 to exit the loop
    else:
        save_sales_data_to_file(raw_dir, date, page, sales_data)
        page += 1

