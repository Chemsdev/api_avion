import mysql.connector
from fastapi import FastAPI
import pandas as pd 

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
    'DISTANCE',
    'DEP_DELAY'
]
                        

# Initialisation de l'app FastAPI/connexion BDD.
app = FastAPI()
cnx = mysql.connector.connect(
    user="chemsdine", 
    password="Ounissi69800", 
    host="chemsdineserver.mysql.database.azure.com", 
    port=3306, 
    database="retard_avion", 
    ssl_disabled=False
)
cursor = cnx.cursor()   

# ========================================================== REQUETES SQL ==================================================================>

# Fonction permettant d'insérer les données.
def insert_data_to_database(data, table1, table2, columns_features, connexion=cnx, cursor=cursor):
    try:
        value_features = list(data.values()) 
        del value_features[-1]
        table1_sql = f"INSERT INTO {table1} ({', '.join(columns_features)}) VALUES ({', '.join(['%s' for _ in range(len(columns_features))])})"
        cursor.execute(table1_sql, value_features)
        inserted_id = cursor.lastrowid
        table2_columns = ["id_fk", "y_pred"]
        if data["Prediction"] == 1:
            table2_values = [inserted_id, "en retard"]
        else:
            table2_values = [inserted_id, "à l'heure"]
        table2_sql = f"INSERT INTO {table2} ({', '.join(table2_columns)}) VALUES ({', '.join(['%s' for _ in range(len(table2_columns))])})"
        cursor.execute(table2_sql, table2_values)
        print("Données insérées avec succès.")
        connexion.commit()
    except Exception as e:
        print(e)
        
# ========================================================================>

# Fonction pour récupérer les données depuis la base de données MySQL.
def get_data_from_database(table1, table2, cursor=cursor):
    cursor.execute(f"SELECT * FROM {table1}")
    data1 = cursor.fetchall()
    cursor.execute(f"SELECT * FROM {table2}")
    data2 = cursor.fetchall()
    return data1, data2

# ========================================================================>

# Fonction pour supprimer les données depuis la base de données MySQL.
def delete_data_from_database(liste, connexion=cnx):
    cursor = connexion.cursor()
    for i in liste:
        cursor.execute(f"DELETE FROM {i}")
    connexion.commit()
    
# ============================================================= @ROUTES ======================================================================>

# Route pour envoyer des données via une requête POST (before).
@app.post("/data/post/before")
async def send_data(data:dict):
    insert_data_to_database(
        data=data, 
        table1="before_takeoff", 
        table2="prediction_before_takeoff",
        columns_features=columns_features_before_takeoff
        )
    return {"message": "Données insérées avec succès"}

# ========================================================================>

# Route pour envoyer des données via une requête POST (after).
@app.post("/data/post/after")
async def send_data(data:dict):
    insert_data_to_database(
        data=data, 
        table1="after_takeoff", 
        table2="prediction_after_takeoff",
        columns_features=columns_features_after_takeoff
    )
    return {"message": "Données insérées avec succès"}

# ========================================================================>

# Route pour récupérer les données via une requête GET (before).
@app.get("/data/get/before")
async def get_data():
    data = get_data_from_database(
        table1="before_takeoff", 
        table2="prediction_before_takeoff"
    )
    return {"data": data}

# ========================================================================>

# Route pour récupérer les données via une requête GET (after).
@app.get("/data/get/after")
async def get_data():
    data = get_data_from_database(
        table1="after_takeoff", 
        table2="prediction_after_takeoff"
    )
    return {"data": data}

# ========================================================================>

# Fonction pour supprimer toutes les données.
@app.delete("/data/delete")
async def delete_data():
    delete_data_from_database(
        liste=[
            "prediction_after_takeoff", 
            "prediction_before_takeoff", 
            "before_takeoff", 
            "after_takeoff"
        ]
    )
    return "Données supprimées avec succès."

# ========================================================================>
