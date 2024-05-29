from flask import Flask, request, jsonify
import os
import pandas as pd


"""
os - interacting with the operating system (create files, directories, management of both)
pandas - data manipulation and analysis (csv, excel, SQL DB)
pyarrow - cross-language. platform independent, in-memory data format.
        Interface for working with different data sources and data types (CSV, Parquet, Json, Pandas Dataframes)

"""
app = Flask(__name__)

reports_dir = 'Reports/FTF/RT91' # onde existem os reports
os.makedirs(reports_dir, exist_ok=True) # garantir que existe

@app.get('/api/reports/ftf/rt91')
def get_reports():
    # lista de todos os ficheiros que acabam em '.parquet' 
    reports = [] 
    for file in os.listdir(reports_dir):
        if file.endswith('.parquet'):
            reports.append(file)
    
    if not reports: # verificar se existe report 
        return jsonify({'error': 'Report not found'}), 404
        
    return jsonify(reports), 200 # retorna um json com o nome dos reports 

@app.post('/api/reports/ftf/rt91')
def create_report():
    # Verificar se recebe um JSON 
    if not request.is_json:
        return jsonify({'error': "Bad Request"}), 400

    data = request.get_json() # parse automático do json, converte num dicionário de py
    if data is None or not {'StartAt', 'EndAt'}.issubset(data):
        return jsonify({'error': 'Invalid JSON'}), 400
    
    start_date = data.get('StartAt')
    end_date = data.get('EndAt') 
    filename = f'RT91-{start_date}_{end_date}.csv'
    report_path = os.path.join(reports_dir, filename)

    df = pd.DataFrame(data['data'])
    df.to_csv(report_path)

    return jsonify({'message': 'Report Created Successfully', 'filename': filename}), 201

@app.put('/api/reports/ftf/rt91/<string:filename>') # ficheiro no endpoint
def update_report(filename):
    report_path = os.path.join(reports_dir, filename)
    # Verificar se existe e se o file acaba em .parquet
    if not os.path.exists(report_path) or not filename.endswith('.parquet'):
        return jsonify({'error': 'Report Not Found'}), 404
    
    data = request.get_json()
    # Garantir que JSON é válido
    if not {'Id', 'Column', 'Value'}.issubset(data):
        return jsonify({'error': 'Invalid Data Format'}), 400
    
    try:
        df = pd.read_parquet(report_path)
        if data['Column'] not in df.columns or not (df['Id'] == data['Id']).any(): # comparar df ao json
            return jsonify({'error': 'Invalid Column or Id'}), 400
        
        # update do df utilizando o json validado
        df.loc[df['Id'] == data['Id'], data['Column']] = data['Value']
        df.to_parquet(report_path)
    except:
        return jsonify({'error': 'An Error Occurred'}), 500
    
    return jsonify({'message': 'Report Updated Successfully'}), 200

@app.delete('/api/reports/ftf/rt91/<string:filename>')
def delete_report(filename):

    report_path = os.path.join(reports_dir, filename) 
    if not os.path.exists(report_path) or not filename.endswith('.parquet'): 
        return jsonify({'error': 'Report Not Found'}), 404
    
    try:
        os.remove(report_path) # os.remove remove o ficheiro no path indicado
    except:
        return jsonify({'error': 'An Error Ocurred'}), 500
    
    return jsonify({'message': 'Report Deleted Successfully'}), 200

if __name__ == "__main__":
    app.run(debug=True)