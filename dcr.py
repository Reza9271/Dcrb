import os
import mysql.connector

def create_files_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS all_files (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_name VARCHAR(255),
                full_path VARCHAR(512),
                file_type VARCHAR(50),
                file_size INT,
                content TEXT
            )
        """)
        print("Table 'all_files' created successfully")
    except mysql.connector.Error as error:
        print("Error creating table:", error)

def insert_files_into_table(directory, connection):
    try:
        cursor = connection.cursor()
        batch_size = 10000
        files_inserted = 0
        
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                file_name, file_extension = os.path.splitext(file)
                
                # Check if the file extension is one of the allowed formats
                if file_extension.lower() in ['.png', '.jpeg', '.jpg', '.svg']:
                    content = None
                else:
                    # If the file format is not supported, set content to file content
                    content = read_file_content(file_path)
                    # Truncate content if it's longer than 10000 characters for HTML files
                    if file_extension.lower() == '.html':
                        content = content[:10000]
                    
                file_size = os.path.getsize(file_path)
                
                cursor.execute("""
                    INSERT INTO all_files (file_name, full_path, file_type, file_size, content)
                    VALUES (%s, %s, %s, %s, %s)
                """, (file_name, file_path, file_extension, file_size, content))
                
                files_inserted += 1
                
                # Commit transaction every batch_size insertions
                if files_inserted % batch_size == 0:
                    connection.commit()
        
        # Commit any remaining rows
        connection.commit()
        
        print("Files inserted into 'all_files' table successfully")
        
        # If there are remaining files not inserted in the last batch, insert them
        remaining_files = files_inserted % batch_size
        while remaining_files > 0:
            for root, _, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    file_name, file_extension = os.path.splitext(file)
                    
                    # Check if the file extension is one of the allowed formats
                    if file_extension.lower() in ['.png', '.jpeg', '.jpg']:
                        content = None
                    else:
                        # If the file format is not supported, set content to file content
                        content = read_file_content(file_path)
                        # Truncate content if it's longer than 10000 characters for HTML files
                        if file_extension.lower() == '.html':
                            content = content[:10000]
                        
                    file_size = os.path.getsize(file_path)
                    
                    cursor.execute("""
                        INSERT INTO all_files (file_name, full_path, file_type, file_size, content)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (file_name, file_path, file_extension, file_size, content))
                    
                    remaining_files -= 1
                    if remaining_files == 0:
                        break  # Exit the loop if all remaining files are inserted
        
        connection.commit()
        print("Remaining files inserted into 'all_files' table successfully")
        
    except mysql.connector.Error as error:
        print("Error inserting files:", error)




def read_file_content(file_path):
    try:
        with open(file_path, 'rb') as f:
            # Read the first 1000 characters of the file
            content = f.read(1000).decode('utf-8', errors='ignore')
            return content
    except Exception as e:
        print(f"Error reading file content: {e}")
        return None

def create_search_results_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_results (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_id INT,
                file_name VARCHAR(255),
                full_path VARCHAR(512),
                file_type VARCHAR(50),
                file_size INT,
                occurrences INT,
                FOREIGN KEY (file_id) REFERENCES all_files(id) ON DELETE CASCADE
            )
        """)
        print("Table 'search_results' created successfully")
    except mysql.connector.Error as error:
        print("Error creating table:", error)

def reset_search_results_id(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            ALTER TABLE search_results AUTO_INCREMENT = 1
        """)
        print("Search results ID reset successfully")
    except mysql.connector.Error as error:
        print("Error resetting search results ID:", error)

def search_and_save(connection, search_string):
    try:
        cursor = connection.cursor()
        
        # Delete previous search results
        cursor.execute("""
            DELETE FROM search_results
        """)
        
        # Reset search results ID to start from 1
        reset_search_results_id(connection)
        
        # Search for files by name in the first table
        cursor.execute("""
            SELECT id, file_name, full_path, file_type, file_size, content 
            FROM all_files 
            WHERE file_name LIKE %s OR full_path LIKE %s OR file_type LIKE %s OR content LIKE %s
        """, ('%' + search_string + '%', '%' + search_string + '%', '%' + search_string + '%', '%' + search_string + '%'))
        
        search_results = cursor.fetchall()
        if search_results:
            for result in search_results:
                file_id, file_name, full_path, file_type, file_size, content = result
                
                # Count occurrences of search string in content
                occurrences = content.count(search_string) if content else 0
                
                # Insert search results into second table
                cursor.execute("""
                    INSERT INTO search_results (file_id, file_name, full_path, file_type, file_size, occurrences)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (file_id, file_name, full_path, file_type, file_size, occurrences))
            connection.commit()
            print("Search results saved into 'search_results' table successfully")
        else:
            print("No files found with the specified name.")
    except mysql.connector.Error as error:
        print("Error searching and saving:", error)

# Connect to MySQL database
try:
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="reza1992!@",
        database="all_files"
    )

    if connection.is_connected():
        print("Connected to MySQL database")

    # Create 'all_files' table if not exists
    create_files_table(connection)

    # Insert files into 'all_files' table
    search_directory = r"C:\Users\REZA\Level1"  # Change this to your directory
    insert_files_into_table(search_directory, connection)

    # Create 'search_results' table if not exists
    create_search_results_table(connection)

    # Search for files by name and save results in 'search_results' table
    search_string = input("Enter the search string: ")
    search_and_save(connection, search_string)

except mysql.connector.Error as error:
    print("Error connecting to MySQL database:", error)