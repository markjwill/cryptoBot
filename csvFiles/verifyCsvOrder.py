import csv

def is_ascending(column):
    for i in range(len(column) - 1):
        if column[i] > column[i + 1]:  # Use '>=' instead of '>' for allowing duplicates
            print(f'index: {i} 1st val: {column[i]} 2nd val: {column[i + 1]}')
            return False
    return True

def is_ascending_no_duplicates(column):
    for i in range(len(column) - 1):
        if column[i] >= column[i + 1]:  # Use '>=' instead of '>' for allowing duplicates
            print(f'index: {i} 1st val: {column[i]} 2nd val: {column[i + 1]}')
            return False
    return True

def test_csv(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # skip header if exists
        column_4_values = []
        for row in reader:
            column_4_values.append(int(row[3]))  # Assuming 4th column index is 3
        if not is_ascending(column_4_values):
            print("Values in the 4th column are out of order.")
            exit()
                
        column_5_values = []
        for row in reader:
            column_5_values.append(int(row[4]))  # Assuming 5th column index is 4
        if not is_ascending_no_duplicates(column_5_values):
            print("Values in the 5th column are out of order.")
            exit()
        return True

file_path = 'tradeData.csv'  # Replace with the path to your CSV file
result = test_csv(file_path)

print("Values in the 4th & 5th column are in ascending order.")
