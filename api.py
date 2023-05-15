import mysql.connector
from fastapi import FastAPI

# Pour lancer l'application et l'API
# 1) execute : uvicorn api:app --reload.
# 2) execute : run streamlit main.py.


# Colonnes de la table before.
columns_features_before_takeoff=[
    'MONTH', 
    'DAY_OF_MONTH', 
    'DAY_OF_WEEK',
    'CRS_DEP_TIME',
    'CRS_ARR_TIME',
    'CRS_ELAPSED_TIME',
    'CARRIER',
    'DISTANCE'
]

# Colonnes de la table after.
columns_features_after_takeoff=[
    'MONTH', 
    'DAY_OF_MONTH', 
    'DAY_OF_WEEK',
    'CRS_DEP_TIME',
    'CRS_ARR_TIME',
    'CRS_ELAPSED_TIME',
    'CARRIER',
    'DISTANCE',
    'DEP_DELAY'
]

                                       
# ======================================================================================================================>

# Initialisation de l'app FastAPI/connexion BDD.
app = FastAPI()
cnx = mysql.connector.connect(
    user="chemsdine", 
    password="Ounissi69800", 
    host="chemsdineserver.mysql.database.azure.com", 
    port=3306, 
    database="avion_retard", 
    ssl_disabled=False
)
cursor = cnx.cursor()   

# ======================================================================================================================>

# Fonction permettant d'insérer les données.
def insert_data_to_database(data, y_pred="oui", connexion=cnx, cursor=cursor):
    try:
        value_features = list(data.values()) 
        if len(value_features) == 8:
            columns_features = columns_features_before_takeoff
            table_name_1 = "before_takeoff"
            table_name_2 = "prediction_before_takeoff"
        else:
            columns_features = columns_features_after_takeoff
            table_name_1 = "after_takeoff"
            table_name_2 = "prediction_after_takeoff"
        table1_sql = f"INSERT INTO {table_name_1} ({', '.join(columns_features)}) VALUES ({', '.join(['%s' for _ in range(len(columns_features))])})"
        cursor.execute(table1_sql, value_features)
        inserted_id = cursor.lastrowid
        table2_columns = ["id_fk", "y_pred"]
        table2_values = [inserted_id, y_pred]
        table2_sql = f"INSERT INTO {table_name_2} ({', '.join(table2_columns)}) VALUES ({', '.join(['%s' for _ in range(len(table2_columns))])})"
        cursor.execute(table2_sql, table2_values)
        print("Données insérées avec succès.")
        connexion.commit()
    except Exception as e:
        print(e)

# Route pour envoyer des données via une requête POST.
@app.post("/data/post")
async def send_data(data:dict):
    insert_data_to_database(data=data)
    return {"message": "Données insérées avec succès"}
