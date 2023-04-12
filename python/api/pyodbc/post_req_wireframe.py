import pyodbc # Import the pyodbc library for connecting to SQL Server
from flask import Flask, request, jsonify # Import Flask for building the API, request for handling HTTP requests, and jsonify for returning JSON responses

app = Flask(__name__) # Create a Flask application

# Configure database connection
server = 'localhost'  # Change this to your SQL Server hostname
database = 'mydb'  # Change this to your database name
username = 'myuser'  # Change this to your username
password = 'mypassword'  # Change this to your password

# Define an API endpoint to accept incoming data via POST requests
@app.route('/transaction', methods=['POST'])
def ingest_transaction():
    # Get data from request
    transaction_id = request.form.get('transaction_id') # Extract 'transaction_id' from the request form data
    timestamp = request.form.get('timestamp') # Extract 'timestamp' from the request form data
    transaction_amt = request.form.get('transaction_amt') # Extract 'transaction_amt' from the request form data
    transaction_desc = request.form.get('transaction_desc') # Extract 'transaction_desc' from the request form data
    transaction_type = request.form.get('transaction_type') # Extract 'transaction_type' from the request form data

    # Insert data into SQL Server
    try:
        # Connect to the SQL Server database
        conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
        cursor = conn.cursor() # Create a cursor to execute SQL queries

        # Execute an INSERT query with parameterized values to prevent SQL injection attacks
        cursor.execute("INSERT INTO transactions (transaction_id, timestamp, transaction_amt, transaction_desc, transaction_type) VALUES (?, ?, ?, ?, ?)",
                       (transaction_id, timestamp, transaction_amt, transaction_desc, transaction_type))
        conn.commit() # Commit the transaction to the database
        cursor.close() # Close the cursor
        conn.close() # Close the database connection
        return jsonify({'message': 'Data ingested successfully.'}), 200 # Return a JSON response indicating success
    except Exception as e:
        return jsonify({'error': str(e)}), 500 # Return a JSON response indicating an error occurred

if __name__ == '__main__':
    app.run(debug=True) # Run the Flask application in debug mode for development purposes
