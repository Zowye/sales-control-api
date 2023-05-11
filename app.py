from flask import Flask, render_template, request, redirect, url_for
import sqlite3

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from monthdelta import monthdelta

app = Flask(__name__)















@app.route('/add')
def index():
    return render_template('add2.html')



@app.route('/insert_data', methods=['POST'])
def insert_data():
    # Obtenha os valores do formulário
    client_name = request.form['client_name']
    installments_number = request.form['installments_number']
    value = request.form['value']
    payment_type = request.form['payment_type']
    mdr = request.form['mdr']
    CMA = request.form['CMA']
    observation = request.form['observation']
    date = request.form['date']
    
    # Conecte-se ao banco de dados
    conn = sqlite3.connect('installments_db')
    c = conn.cursor()
    
    # Insira os valores na tabela sales_control
    c.execute("INSERT INTO sales_control (client_name, installments_number, value, payment_type, mdr, CMA, observation, date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (client_name, installments_number, value, payment_type, mdr, CMA, observation, date))
    
    # Salve as mudanças e desconecte do banco de dados
    conn.commit()
    conn.close()
    
    # Redirecione para a página inicial ou uma página de sucesso
    return redirect('/')








@app.route('/')
def view():
    conn = sqlite3.connect('installments_db')
    cursor = conn.cursor()
    QUERY = """
            SELECT payment_id, date, client_name, installments_number, value, CMA, mdr, observation
            FROM sales_control
            """

    cursor.execute(QUERY)
    results = cursor.fetchall()
    conn.close()
    return render_template('view_all.html', registros=results)




@app.route('/adicionar', methods=['POST'])
def adicionar_registro():
    conn = sqlite3.connect('installments_db')
    cursor = conn.cursor()

    # obtem os dados do formulario
    data = request.form['date']
    valor = request.form['value']
    num_parcelas = request.form['installments_number']
    taxa_juros = request.form['rate']

    # insere um novo registro na tabela
    cursor.execute('INSERT INTO installments (date, value, installments_number, rate) VALUES (?, ?, ?, ?)', (data, valor, num_parcelas, taxa_juros))

    # salva as mudancas no banco de dados
    conn.commit()

    # fecha a conexão e o cursor
    cursor.close()
    conn.close()

    # redireciona para a pagina inicial
    return redirect(url_for('view'))











@app.route("/delete", methods=["POST"])
def delete():
    ids = request.get_json()

    conn = sqlite3.connect('installments_db')
    cursor = conn.cursor()

    for id in ids:
        cursor.execute("DELETE FROM sales_control WHERE payment_id = ?", (id,))
    
    conn.commit()
    conn.close()

    return ""








@app.route('/get_values')
def get_values():
    conn = sqlite3.connect('installments_db')

    # Crie um cursor para executar consultas SQL
    cursor = conn.cursor()

    QUERY = """
        SELECT payment_id, date, value, installments_number, mdr
        FROM sales_control
            """

    # Execute uma consulta SQL para retornar os nomes de todas as tabelas no banco de dados
    cursor.execute(QUERY)
    resultado = cursor.fetchall()


    # converter a lista de tuplas em um DataFrame
    df = pd.DataFrame(resultado, columns=['payment_id', 'date', 'value', 'installments_number', 'mdr'])
    df['installment_value'] = round(df['value'] / df['installments_number'], 2)
    df['date'] = pd.to_datetime(df['date']).dt.date


    # QUERY INTO THE DATAFRAME
    def get_max_date(N, date):
        MAX_DATE = (date + timedelta(days=int(N*30)))
        return MAX_DATE



    # Creating individual dataframe with dates for each payment register
    def transform_register(x):
        # Collecting the register values
        FIRST_DATE = x['date']
        N = x['installments_number']
        PAYMENT_ID = x['payment_id']

        LIST_DATES_REGISTER = (pd.date_range(FIRST_DATE, periods=N, freq='MS') + timedelta(days=FIRST_DATE.day-1)).date

        df_aux = pd.DataFrame({'date': LIST_DATES_REGISTER})
        df_aux['value'] = x['installment_value']

        df_aux = df_aux.pivot_table(index=None, columns='date', values='value')
        
        df_aux['PAYMENT_ID'] = PAYMENT_ID

        return df_aux.set_index('PAYMENT_ID')




    MAX_DATE = df.apply(lambda row : get_max_date(row['installments_number'], row['date']), axis=1).max()

    # ONE MORE MONTH FOR SAFETY REASONS
    MAX_DATE = MAX_DATE + timedelta(30)
    MIN_DATE = df['date'].min()
    DATES = pd.date_range(MIN_DATE, MAX_DATE, freq='D').date

    df_total = pd.DataFrame(columns=DATES)



    # Concatenating all the registers
    for index, row in df.iterrows():
        df_payment_info = transform_register(row)
        df_total = pd.concat([df_total, df_payment_info], axis=0)
        
    df_t = df_total.fillna(0)
    df_to_plot = df_t.sum().reset_index().rename(columns={'index':'Data', 0:'Valor_a_Receber'})


    df_to_plot = df_to_plot[df_to_plot['Valor_a_Receber'] != 0]

    # render the DataFrame as an HTML table
    table_html = df_to_plot.to_html(index=False)


    # render the HTML template with the table
    return render_template('table.html', table=table_html)








if __name__ == '__main__':
    app.run()