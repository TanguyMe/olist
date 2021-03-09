# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import pymysql
from sqlalchemy.types import Integer, String, Float, Date, DateTime

# Create dataframes from the csv files
df_customers = pd.read_csv("Data_olist/olist_customers_dataset.csv")
df_geolocation = pd.read_csv("Data_olist/olist_geolocation_dataset.csv")
df_order_items = pd.read_csv("Data_olist/olist_order_items_dataset.csv")
df_order_payments = pd.read_csv("Data_olist/olist_order_payments_dataset.csv")
df_order_reviews = pd.read_csv("Data_olist/olist_order_reviews_dataset.csv")
df_orders = pd.read_csv("Data_olist/olist_orders_dataset.csv")
df_products = pd.read_csv("Data_olist/olist_products_dataset.csv")
df_sellers = pd.read_csv("Data_olist/olist_sellers_dataset.csv")

#Get informations about the datas read from olist_geolocation_dataset
df = df_geolocation
index=df.index
rows=len(index)
print("Nombre de lignes :", rows) #Display the number of rows
print(df.head()) #Display the 5 first rows
print(df.dtypes) #Display column types
#Display the column names and their maximum length
print("Dataset geolocation : ",dict([(v, df_geolocation[v].apply(lambda r: len(str(r)) if r!=None else 0).max())for v in df_geolocation.columns.values]))




print("Dataset customer : ",dict([(v, df_customers[v].apply(lambda r: len(str(r)) if r!=None else 0).max())for v in df_customers.columns.values]))
print("Dataset geolocation : ",dict([(v, df_geolocation[v].apply(lambda r: len(str(r)) if r!=None else 0).max())for v in df_geolocation.columns.values]))
print("Dataset order_items : ",dict([(v, df_order_items[v].apply(lambda r: len(str(r)) if r!=None else 0).max())for v in df_order_items.columns.values]))
print("Dataset order_payments : ",dict([(v, df_order_payments[v].apply(lambda r: len(str(r)) if r!=None else 0).max())for v in df_order_payments.columns.values]))
print("Dataset order_reviews : ",dict([(v, df_order_reviews[v].apply(lambda r: len(str(r)) if r!=None else 0).max())for v in df_order_reviews.columns.values]))
print("Dataset orders : ",dict([(v, df_orders[v].apply(lambda r: len(str(r)) if r!=None else 0).max())for v in df_orders.columns.values]))
print("Dataset products : ",dict([(v, df_products[v].apply(lambda r: len(str(r)) if r!=None else 0).max())for v in df_products.columns.values]))
print("Dataset sellers : ",dict([(v, df_sellers[v].apply(lambda r: len(str(r)) if r!=None else 0).max())for v in df_sellers.columns.values]))

We delete the rows with duplicate geolocation_zip_code_prefix, geolocation_lat, geolocation_lng
df_geolocation = df_geolocation.drop_duplicates(subset=['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'])

#Connection to MySQL
mydb = mysql.connector.connect(
  host="localhost",
  user="student"
)

#Cursor initialization
mycursor = mydb.cursor()
#Database creation
mycursor.execute("CREATE DATABASE IF NOT EXISTS olist;")


mydb = mysql.connector.connect(
  host="localhost",
  user="student",
  database="olist"
)


#Create the engine to connect python and mysql
engine = create_engine('mysql+pymysql://student@localhost/olist')
#Create the tables
try : #Try to create customers
  df_customers.to_sql(name="Customers", con=engine, index=False, if_exists='fail', dtype={'customer_id': String(32),
  'customer_unique_id' : String(32), 'customer_zip_code_prefix' : String(5), 'customer_city' : String(32), 'customer_state' : String(2)})
except ValueError: #If it already exists, notify the user
  print("Customers existe déjà")

try:
  df_geolocation.to_sql(name="Geolocation", con=engine, index=False, if_exists='fail', dtype={'geolocation_zip_code_prefix': String(5),
  'geolocation_lat' : String(19), 'geolocation_lng' : String(19), 'geolocation_city' : String(38), 'geolocation_state' : String(2)})
except ValueError:
  print("Geolocation existe déjà")

try :
  df_order_items.to_sql(name="Order_items", con=engine, index=False, if_exists='fail', dtype={'order_id': String(32),
  'order_item_id' : Integer, 'product_id' : String(32), 'seller_id' : String(32), 'shopping_limit_date' : Date, 'price' : Float(7,2,None), 'freight_value' : Float(6,2,None)})
except ValueError:
  print("Order_items existe déjà")

try :
  df_order_payments.to_sql(name="Order_payments", con=engine, index=False, if_exists='fail', dtype={'order_id': String(32),
  'payment_sequential' : Integer, 'payment_type' : String(11), 'payment_installments' : Integer, 'payment_value' : Float(10,2,None)})
except ValueError:
  print("Order_payments existe déjà")

try :
  df_order_reviews.to_sql(name="Order_reviews", con=engine, index=False, if_exists='fail', dtype={'review_id': String(32),
  'order_id' : String(32), 'review_score' : Integer, 'review_comment_title' : String(26), 'review_comment_message' : String(208), 'review_creation_date' : DateTime,
  'review_answer_timestamp': DateTime})
except ValueError:
  print("Order_reviews existe déjà")

try :
  df_orders.to_sql(name="Orders", con=engine, index=False, if_exists='fail', dtype={'order_id': String(32),
  'customer_id' : String(32), 'order_status' : String(11), 'order_purchase_timestamp' : DateTime, 'order_approved_at' : DateTime,
  'order_delivered_carrier_date' : DateTime, 'order_delivered_customer_date' : DateTime, 'order_estimated_delivery_date' : DateTime})
except ValueError:
  print("Orders existe déjà")

try:
  df_products.to_sql(name="Products", con=engine, index=False, if_exists='fail', dtype={'product_id': String(32),
  'product_category_name' : String(46), 'product_name_lenght' : Float(8,4,None), 'product_description_lenght' : Float(12,6,None),
  'product_photos_qty' : Float(8,4,None), 'product_weight_g' : Float(14,7,None), 'product_length_cm' : Float(10,5,None),
  'product_height_cm' : Float(10,5,None), 'product_width_cm' : Float(10,5,None)})
except ValueError:
  print("Products existe déjà")

try :
  df_sellers.to_sql(name="Sellers", con=engine, index=False, if_exists='fail', dtype={'seller_id': String(32),
  'seller_zip_code_prefix' : String(5), 'seller_city' : String(40), 'seller_state' : String(2)})
except ValueError:
  print("Sellers existe déjà")

#Permet de tester l'unicité des attributs d'une table
def test_unicite_attribut(table, attribut):
  mycursor = mydb.cursor()
  print("SELECT {0} FROM {1} GROUP BY {0} HAVING COUNT(({0}))>1".format(attribut, table))
  mycursor.execute("SELECT {0} FROM {1} GROUP BY {0} HAVING COUNT(({0}))>1".format(attribut, table))
  for x in mycursor:
    print(x)


#Permet d'ajouter les clés primaires et secondaires
def ajout_cles():
    mycursor=mydb.cursor()
    #Add customer_id as primary key in table Customers
    mycursor.execute("ALTER TABLE Customers ADD PRIMARY KEY (customer_id)")
    mycursor.execute("ALTER TABLE Orders ADD PRIMARY KEY (order_id)")
    mycursor.execute("ALTER TABLE Products ADD PRIMARY KEY (product_id)")
    mycursor.execute("ALTER TABLE Sellers ADD PRIMARY KEY (seller_id)")

    #Add customer_id as foreign key in table Orders
    mycursor.execute("ALTER TABLE Orders ADD FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)")
    mycursor.execute("ALTER TABLE Order_reviews ADD FOREIGN KEY (order_id) REFERENCES Orders(order_id)")
    mycursor.execute("ALTER TABLE Order_payments ADD FOREIGN KEY (order_id) REFERENCES Orders(order_id)")
    mycursor.execute("ALTER TABLE Order_items ADD FOREIGN KEY (order_id) REFERENCES Orders(order_id")
    mycursor.execute("ALTER TABLE Order_items ADD FOREIGN KEY (product_id) REFERENCES Products(product_id)")
    mycursor.execute("ALTER TABLE Order_items ADD FOREIGN KEY (seller_id) REFERENCES Sellers(seller_id)")


#Effectue les requetes SQL demandées
def requetes_sql():
    mycursor=mydb.cursor()

    mycursor.execute("SELECT COUNT(DISTINCT customer_unique_id) FROM Customers")
    clients=mycursor.fetchall()
    print("Nombre de client total :", clients)

    mycursor.execute("SELECT COUNT(DISTINCT product_id) FROM Products")
    produit=mycursor.fetchall()
    print("Nombre de produit total :", produit)

    mycursor.execute("""SELECT product_category_name, COUNT(DISTINCT product_id) FROM Products 
                        GROUP BY product_category_name""")
    produits_par_categorie=mycursor.fetchall()
    print("Nombre de produits par catégories :", '\n'.join([str(x) for x in produits_par_categorie]), sep='\n')

    mycursor.execute("SELECT COUNT(DISTINCT order_id) FROM Orders")
    commande=mycursor.fetchall()
    print("Nombre de commandes :", commande)

    mycursor.execute("""SELECT order_status, COUNT(DISTINCT order_id) FROM Orders 
                        GROUP BY order_status""")
    commande_par_etat=mycursor.fetchall()
    print("Nombre de commandes par état :", commande_par_etat)

    mycursor.execute("""SELECT EXTRACT(MONTH FROM order_purchase_timestamp) AS month,
                        EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
                        COUNT(DISTINCT order_id) 
                        FROM Orders 
                        GROUP BY year, month
                        ORDER BY year, month""")
    commande_par_mois=mycursor.fetchall()
    print("Nombre de commandes par mois :", commande_par_mois)

    mycursor.execute("""WITH paymult AS 
                        (
                            SELECT order_id, SUM(payment_value) AS somme 
                            FROM Order_payments 
                            GROUP BY order_id
                        ) 
                        SELECT AVG(somme) FROM paymult""")
    prix_moyen_commande=mycursor.fetchall()
    print("Prix moyen d'une commande :", prix_moyen_commande)

    mycursor.execute("SELECT AVG(review_score) FROM Order_reviews")
    score_satisfaction_moyen=mycursor.fetchall()
    print("Score de satisfaction moyen :", score_satisfaction_moyen)

    mycursor.execute("SELECT COUNT(DISTINCT seller_id) FROM Sellers")
    nombre_vendeur=mycursor.fetchall()
    print("Nombre vendeur:", nombre_vendeur)

    mycursor.execute("""SELECT seller_state, COUNT(DISTINCT seller_id) FROM Sellers 
                        GROUP BY seller_state""")
    nombre_vendeur_par_region=mycursor.fetchall()
    print("Nombre vendeur par région:", nombre_vendeur_par_region)

    mycursor.execute("""SELECT pr.product_category_name, COUNT(it.product_id) FROM Products AS pr
                        JOIN Order_items AS it ON it.product_id=pr.product_id
                        GROUP BY pr.product_category_name
                        ORDER BY pr.product_category_name""")
    produit_vendu_par_categorie=mycursor.fetchall()
    print("Quantité de produits vendus par catégorie:", produit_vendu_par_categorie)

    mycursor.execute("""SELECT DATE(order_purchase_timestamp), COUNT(DISTINCT order_id) FROM Orders
                        GROUP BY DATE(order_purchase_timestamp)""")
    commande_par_jour=mycursor.fetchall()
    print("Nombre de commandes par jour:", commande_par_jour)

    mycursor.execute("""SELECT AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) FROM Orders""")
    ecart_commande_livraison=mycursor.fetchall()
    print("Durée moyenne entre la commande et la livraison:", ecart_commande_livraison)

    mycursor.execute("""SELECT se.seller_city, COUNT(DISTINCT it.order_id) FROM Sellers AS se
                        JOIN Order_items AS it ON it.seller_id = se.seller_id
                        GROUP BY se.seller_city
                        ORDER BY se.seller_city
                        """)
    commande_par_ville=mycursor.fetchall()
    print("Nombre de commandes par jour:", commande_par_ville)

    mycursor.execute("""WITH paymult AS
                        (
                            SELECT order_id, SUM(payment_value) AS somme
                            FROM Order_payments
                            GROUP BY order_id
                        )
                        SELECT MIN(somme) FROM paymult""")
    prix_min_commande=mycursor.fetchall()
    print("Prix minimum des commandes:", prix_min_commande)

    mycursor.execute("""WITH paymult AS
                        (
                            SELECT order_id, SUM(payment_value) AS somme
                            FROM Order_payments
                            GROUP BY order_id
                        )
                        SELECT MAX(somme) FROM paymult""")
    prix_max_commande=mycursor.fetchall()
    print("Prix maximum des commandes:", prix_max_commande)

    mycursor.execute("""SELECT EXTRACT(MONTH FROM order_purchase_timestamp) AS month,
                        EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
                        AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp))
                        FROM Orders
                        GROUP BY year, month
                        ORDER BY year, month""")
    temps_moyen_livraison_par_mois=mycursor.fetchall()
    print("Temps moyen d'une livraison par mois:", temps_moyen_livraison_par_mois)

requetes_sql()

#Permet d'ajouter et de print un élément dans la table Products
def insert_table():
    mycursor.execute('''
        INSERT INTO Products VALUES ('product_id','product_category_name',4.21,8.77,4.23,44,55,66,77)   
    ''')

    mydb.commit()

    mycursor.execute('''
        SELECT *
        FROM Products
        WHERE product_id = 'product_id'
    ''')
    res = mycursor.fetchall()
    for x in res:
        print(x)
