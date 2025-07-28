import pandas as pd
import os
from flask import Flask, request, render_template, send_file, flash, redirect, url_for
import io

# Initialize the Flask application
app = Flask(__name__)
# A secret key is needed for flashing messages (showing errors to the user)
app.secret_key = 'supersecretkey' 

def process_bvn_data(input_file, sheet_name):
    """
    Processes the BVN data from an uploaded Excel file stream.

    Args:
        input_file (file-like object): The uploaded Excel file.
        sheet_name (str): The name of the sheet to process.

    Returns:
        BytesIO or None: An in-memory bytes buffer containing the processed Excel file,
                         or None if an error occurs.
    """
    try:
        # Load the specified sheet from the uploaded Excel file.
        df = pd.read_excel(input_file, sheet_name=sheet_name)

        # --- Data Cleaning and Preparation ---
        df.rename(columns={
            'T.V.Date': 'TV_Date',
            'Object Code': 'Object_Code',
            'Net Amt': 'Net_Amt'
        }, inplace=True)
        df['TV_Date'] = pd.to_datetime(df['TV_Date'], errors='coerce')
        df.dropna(subset=['TV_Date'], inplace=True)
        df['Net_Amt'] = pd.to_numeric(df['Net_Amt'], errors='coerce').fillna(0)

        unique_dates = df['TV_Date'].dt.date.unique()

        # --- Excel File Creation (in memory) ---
        output_buffer = io.BytesIO()
        with pd.ExcelWriter(output_buffer, engine='xlsxwriter') as writer:
            for date in sorted(unique_dates):
                daily_df = df[df['TV_Date'].dt.date == date].copy()
                processed_rows = []

                for _, row in daily_df.iterrows():
                    net_amount = row['Net_Amt']
                    new_row = {
                        'Object Code': '', 'Net Amount': '', 'Object Code1': '',
                        'Net Amount Again': '', 'Total': net_amount
                    }
                    object_code_str = str(row['Object_Code'])
                    if 'NoObject' in object_code_str or 'Salary' in object_code_str:
                        new_row['Object Code'] = row['Object_Code']
                        new_row['Net Amount'] = net_amount
                    else:
                        new_row['Object Code1'] = row['Object_Code']
                        new_row['Net Amount Again'] = net_amount
                    processed_rows.append(new_row)

                output_df = pd.DataFrame(processed_rows)
                
                if not output_df.empty:
                    total_for_day = daily_df['Net_Amt'].sum()
                    is_salary = daily_df['Object_Code'].astype(str).str.contains('NoObject|Salary', na=False)
                    total_net_amount = daily_df[is_salary]['Net_Amt'].sum()
                    total_net_amount_again = daily_df[~is_salary]['Net_Amt'].sum()
                    total_row = {
                        'Object Code': 'Grand Total', 'Net Amount': total_net_amount,
                        'Object Code1': '', 'Net Amount Again': total_net_amount_again,
                        'Total': total_for_day
                    }
                    total_row_df = pd.DataFrame([total_row])
                    output_df = pd.concat([output_df, total_row_df], ignore_index=True)

                sheet_name_str = date.strftime('%d-%m-%Y')
                output_df.to_excel(writer, sheet_name=sheet_name_str, index=False)
        
        # After writing, reset the buffer's position to the beginning
        output_buffer.seek(0)
        return output_buffer

    except Exception as e:
        # Log the error for debugging
        print(f"An error occurred: {e}")
        return None

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # Check if a file was uploaded
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        
        file = request.files['file']
        
        # Check if the user selected a file
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
            
        # Check if the file is an Excel file
        if file and file.filename.endswith('.xlsx'):
            sheet_name = request.form.get('sheet_name', 'Table 1 (2)')
            processed_file_buffer = process_bvn_data(file, sheet_name)
            
            if processed_file_buffer:
                return send_file(
                    processed_file_buffer,
                    as_attachment=True,
                    download_name='processed_report.xlsx',
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
            else:
                flash(f'Error processing file. Please ensure the sheet "{sheet_name}" exists and the data is correctly formatted.')
                return redirect(request.url)

    # For a GET request, just show the upload page
    return render_template('index.html')

if __name__ == '__main__':
    # This is for local testing only. Render will use a production server.
    app.run(debug=True)
