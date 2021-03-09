





**COMPTE-RENDU** 

**Conception d’une base de données pour un site de e-commerce** 

*Hatice - Marie - Tanguy*





# **INTRODUCTION**





## **CONTEXTE** 

 

Nous intégrons une société de e-commerce brésilienne en tant que développeur IA. Nous devons effectuer une refonte de leur base de données. Pour commencer, nous proposons notre MPD. Ensuite, nous allons utiliser le MPD associé aux données fournies par l’entreprise pour intégrer ces données à MySQL et les exploiter pour calculer une série d’indicateurs. 



## **PARTAGE DU TRAVAIL**



  La conception du MPD est le résultat d’une réflexion commune.

Nous avons travaillé sur les créations de base individuellement, puis nous avons recoupé nos résultats, afin de choisir le plus pertinent.

Pour la création des requêtes d’appels à la base, il s’agissait plus d’un travail d’équipe. Nos niveaux en SQL étant différents, il s’agissait principalement d’un partage de compétences.

Hatice ayant fait le choix d’un outil différent ( utilisation d’une version de pro de pycharm permettant un accès automatisé à la base de donnée via datagrip), elle a dû adapter son code.





# **I - Conception de la DataBase**





**![img](https://lh5.googleusercontent.com/so4zLTxAIqSewzbyLvhkrR6gUdegwlnj9euGbUolxh9qgVCJ8WNV5TYWRBsnMzKZqoaPXFRN7oMQvEjua3Ee8_nkRB3ksj_ks7kZDQCoVxY6ecY3lRti4u8rnvsJuKgt3R_imgKq)**

*Schéma de la BDD existante chez le client*



**MPD** 

**![img](https://lh3.googleusercontent.com/f1gZHk53kt0mFLPeKXZF-z4_2y1_lGQ8ckTiX_vxTOBHvaNHquB7x36ToNkJKSyvloqpp3Hy9Q03BOrW33u485bh1R8u2uVrIHWFkDr8YIJDCOnoD2hTKoZ7YpgX-5XU_I90Oeup)**

*MPD proposé au client (créé via draw.io)*







# **II - Création de la DataBase**





## **TECHNOLOGIES UTILISÉES** 



  Nous avons fait notre diagramme de classe et notre UML sur draw.io. Ensuite, pour créer les bases de données, nous avons utilisé 2 méthodes différentes. 



L’une en utilisant Python sur l’IDE Pycharm avec les librairies suivantes :

- Pandas : Pour lire les fichiers csv et utiliser la fonction tosql() qui permet d’entrer les données lues dans les bases de données
- Sqlalchemy : Pour créer l’engine dont a besoin tosql()
- mysql.connector : Pour travailler sur les bases de données en SQL
- pymysql : Permet de se connecter à une base de données MySQL depuis Python
- sqalchemy.types : Permet d’importer les types de données nécessaires pour utiliser tosql()



L’autre via l’extension DataGrip (IDE de base de données conçu pour répondre aux besoins spécifiques des développeurs) de PyCharm.





## **CREATION ET INTEGRATION DE LA BASE DE DONNÉES:**



Pour DataGrip: les ordres mysql se font directement dans la console : 



Avec Pandas, on commence par entrer les données dans des dataframes. 

*Insertion des données issues des CSV dans des dataframes pour chaque table:*

```python
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import pymysql
from sqlalchemy.types import Integer, String, Float, Date, DateTime

\# Create dataframes from the csv files
df_customers = pd.read_csv("Data_olist/olist_customers_dataset.csv")
df_geolocation = pd.read_csv("Data_olist/olist_geolocation_dataset.csv")
df_order_items = pd.read_csv("Data_olist/olist_order_items_dataset.csv")
df_order_payments = pd.read_csv("Data_olist/olist_order_payments_dataset.csv")
df_order_reviews = pd.read_csv("Data_olist/olist_order_reviews_dataset.csv")
df_orders = pd.read_csv("Data_olist/olist_orders_dataset.csv")
df_products = pd.read_csv("Data_olist/olist_products_dataset.csv")
df_sellers = pd.read_csv("Data_olist/olist_sellers_dataset.csv")


```



  Ensuite, pour déterminer ce qu’on a dans la table, on peut utiliser des outils disponibles avec pandas pour visualiser le nombre de lignes, les 5 premières lignes, les types de chaque colonne, le nom des colonnes ainsi que la longueur maximale des éléments de cette colonne (pour pouvoir fixer une valeur maximum pour les attributs dans SQL). 



*Extraction des informations de la table geolocation:*

```pyhton
#Get informations about the datas read from olist_geolocation_dataset

df = df_geolocation

index=df.index

rows=len(index)

print(rows) #Display the number of rows

print(df.head()) #Display the 5 first rows

print(df.dtypes) #Display column types

#Display the column names and their maximum length

print("Dataset geolocation : ",dict([(v, df_geolocation[v].apply(lambda r: len(str(r)) if r!=None else 0).max())for v in df_geolocation.columns.values]))
```







*Résultat de l’extraction des informations de la table geolocation:*

![img](https://lh5.googleusercontent.com/aV2sQzvqi79i1L69gjuC61buP6ghJfp9a0xwPH2ufImgEwz4R9PLlRBFQ4axldGiZjygFPa1Az-HnhEUzswmkLi8nLAEpPWfDYobvOTGnwQrHUzE2Gnkbrc_nwEtDsVJLFAY80c_)



  On récupère ces informations pour chacune des données, qu’on peut recouper avec la visualisation des données sur Kaggle pour mieux comprendre avec quel type de données on travaille. 



  On peut maintenant commencer à créer la database du SQL grâce à mysql.connector. 



*Création database:*

```python
#Connection to MySQL
mydb = mysql.connector.connect(
 host="localhost",
 user="student"
)

#Cursor initialization
mycursor = mydb.cursor()

#Database creation
mycursor.execute("CREATE DATABASE IF NOT EXISTS olist;")
```



  Ensuite on entre dans la database sur laquelle on va travailler. 



*Entrée dans la database*

```python
mydb = mysql.connector.connect(
 host="localhost",
 user="student",
 database="olist"
)
```



  Grâce à panda et à sa fonction to_sql, on va pouvoir insérer les dataframes issues des fichiers CSV en base de données SQL. On créé un engine via sqlalchemy qui permet de préciser les modalités de connexion entre Python et MySQL. On indique le système vers lequel on veut envoyer nos données MySQL via l’interface pymysql vers la base de données olist en tant qu’utilisateur student. 

  Ensuite, on a une succession de try dans lesquelles se trouvent l’insertion des données sur MySQL. On précise le nom de la table, le mode de connexion, qu’on ne veut pas d’index. Dans le cas où la table existe déjà, on a 3 possibilités:

- Annuler la création (‘fail’) ce qui renvoie une erreur, d’où le except qui permet de notifier l’utilisateur que la table existe déjà 
- Remplacer la table (‘replace’) 
- Ajouter à la table déjà existante (‘append’)

Si on veut réinitialiser les tables, il faut donc remplacer ‘fail’ par ‘replace’. 

  Le dernier élément à préciser dans la fonction est le dtype, qui regroupe le nom des colonnes ainsi que leur type, ces types sont propres à sqlalchemy. On précise le type de l’attribut et la taille qu’on a obtenu de nos observations précédentes.



*Creation of databases in MySQL*

```python
\#Create the engine to connect python and mysql
engine = create_engine('mysql+pymysql://student@localhost/olist')

\#Create the tables
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


```



# **III - Appels à la DataBase**



## Nombre de clients total:

requête:

```python
mycursor.execute("SELECT COUNT(DISTINCT customer_unique_id) FROM Customers")
res = mycursor.fetchone()
total_rows = res[0]
print("\nNombre de clients :")
print(total_rows)
```

résultat en console:

![image-20210309123532613](/home/apprenant/.config/Typora/typora-user-images/image-20210309123532613.png)



## Nombre de produits total:

requête:

```python
mycursor.execute("SELECT COUNT(DISTINCT product_id) FROM Products")
res = mycursor.fetchone()
tot_prod = res[0]
print("\nNombre de produits :")
print(tot_prod)
```



résulat en console:

**![img](https://lh4.googleusercontent.com/jlHI6rEb-UwwvUUHOc29ivOIUl1OTcabW8mH4lIUlF4la4fPPB4-XvnEBpBjjNeR1YdEUzF8jucA9R_tBmbhP_wQpV6a71qb50yjCMjt7r3e_QeKnvDGAzkLM4D8SNzSdwwXPV2x)**



## Nombre de produits par catégorie:

requête:

```python
mycursor.execute('''
SELECT product_category_name,
COUNT(DISTINCT product_id)
FROM Products
GROUP BY product_category_name
''')

res = mycursor.fetchall()

print("\nNombre de produits par catégories :")

i = 0
for x in res:
  i += 1
  print(i, x)
```



résultat en console (extrait):

![img](https://lh6.googleusercontent.com/zMoK4PGTM3orz52LZuMBZr_nbqwjFY9wED-O4DkrYsxkHtJ14_qt2cZdWAa8TNsaE0B9EY1YPqAOem7C0rPzJacswg_aVCHER5bwbe6zqnu95pRcpPSAwTbZN8-EOvIFzt2AFpLj)





## Nombre de commandes total:

requête:

```python
mycursor.execute("""
  SELECT COUNT(DISTINCT order_id)
  FROM Orders
""")

res = mycursor.fetchone()

nb_orders = res[0]

print("\nNombre de commandes :")

print(nb_orders)
```

résultat en console:



![img](https://lh5.googleusercontent.com/pJM1achdDRmOwucfAVtQm3eEVuR8EUMQDR2qBanG6UjKqMf8xTR4aqHkNLKhICKpOma-JnJJQP9Ka88wzQdflJ5SN87QJ7-opyQz2I5quURSNyXGCC_u2tp8GgcI7c4VamzwqxC8)



## Nombre de commandes selon leur état (en cours de livraison etc...):

requête:

```python
mycursor.execute('''
SELECT order_status,
COUNT(DISTINCT order_id)
FROM Orders
GROUP BY order_status
''')

res = mycursor.fetchall()

print("\nNombre de commandes par status :")

i = 0
for x in res:
  i += 1
  print(i, x)
```



résultat en console (intégralité):



![img](https://lh4.googleusercontent.com/X_YVhcXG1oN9bbteny-pqqvk1wG18310IJ3ss61JL8QfqmNwHYe8x4jSlV1pIx8dKKqZRe3AGr4BioGrWFr1y0corTnazMNHMP9yl_2hvl6JLTRppES0aGIEimREnxbtFR59xeD2)



## Nombre de commandes par mois:

requête:

```python
mycursor.execute('''
SELECT count(*),MONTH(order_purchase_timestamp)
FROM Orders
GROUP BY MONTH(order_purchase_timestamp)
''')

res = mycursor.fetchall()

print("\nNombre de commandes par mois :")

i = 0
for x in res:
  i += 1
  print(i, x)
```



résultat en console par mois:



![img](https://lh5.googleusercontent.com/5pMEpPCIdf-EFe09h1xvDZETv9ypLXD6aYakeA1IcdzDz81IFnVNByabKJSzue5XalBeLkhr7pwRZPRt0XQw8rcfG42kfpXWNCq0uIHHlXsw6M9lYwgw4Gzyo6hrakT7EbPj863l)



## Prix moyen d'une commande (panier moyen):

requête:

```python
mycursor.execute('''
SELECT AVG(payment_value)
FROM Order_payments
''')

res = mycursor.fetchall()

print("\nPrix moyen d'une commande (panier moyen) :")
for x in res:
  print(x)
```



résultat en console:



![img](https://lh4.googleusercontent.com/cNwY6Wlh6I8Uz5YeRAzZLFJyP8pNZgzujKhEyf80y0vXJVG1m8BtT9UMRJ9b9xeWw4KkZ7I1hDzt38OBEdKOxcuLTGAtL8GY8MmxR0uPu9KIL81Ciw4TgnQWn2PvvVgO1aqmSqDs)



## Score de satisfaction moyen (notation sur la commande):

requête:

```python
mycursor.execute("""
  SELECT AVG(review_score)
  FROM Order_reviews
""")

res = mycursor.fetchall()
print("\nScore de satisfaction moyen (notation sur la commande):")
print(res[0])
```



résultat en console:

![img](https://lh4.googleusercontent.com/9oDi3mh55jxCFZF9V--XYWt91Ct5ynCq42dqSxjzmU18KI2qa0s1ez_nLW3YJOHSk517d9dCMLBg80im9xLd52_O3YJj48tlVLC-cBRs2aBlHto4Ud_KjiCTkHVuLAKDik6mefxV)



## Nombre de vendeurs:

requête:

```python
mycursor.execute("""
  SELECT *
  FROM Sellers
""")

res = mycursor.fetchall()
print("\nNombre de vendeur: ")
print(len(res))
```



résultat en console:

![img](https://lh4.googleusercontent.com/0UIhOAgH5DUkkf4lCtv1ZqLbUkW6b0wyaPyyyHb0qAYiouugsG-4yoO7PvyILZX5qgX-Siv15IDjSA8R-yDz41Ch0KviM5tqvo2eSiSBTQNgQVF747GBVeQ8cH-rLxrrUjkOaPCd)



## Nombre de vendeurs par région:

requête:

```python
mycursor.execute("""
  SELECT
  DISTINCT seller_city
  FROM Sellers
""")
res = mycursor.fetchall()

print("\nNombre de vendeur par région: ")
i = 0
for x in res:
  i += 1
  print(i, x)
```



résultat en console (extrait):

![img](https://lh5.googleusercontent.com/FE7hpV-iUzlazbUdL11KggY9LDh5S0G-bD2DXYWEKL3s_eAn2qw1nJlg0ejhnRRr1PaQK3-sf8wqPZZmFSZkgsqnDXdFqNDkuF4zEuSfpp-ZmKKbbvoKUCO5blj9kRo2SQF4sf_K)



## Quantité de produits vendus par catégorie:

requête:

```python
mycursor.execute("""
  SELECT COUNT(p.product_id),p.product_category_name
  FROM Products AS p
  INNER JOIN Order_items AS oi
  ON p.product_id = oi.product_id
  GROUP BY p.product_category_name
  ORDER BY p.product_category_name
""")

res = mycursor.fetchall()

print("\nQuantité de produits vendus par catégories: ")
for x in res:
  print(x)
```



résultat en console (extrait):



![img](https://lh6.googleusercontent.com/MDRalEQnJnwGfukwP-NnLlVeSO-HPROnewwoVS2CsFX9GkPT3pVk1uchVa3fzX2uhc4IaJdM-tan4proMuQn8acdaiNBNFeoPsC0AgBAbHVDX-SIVOXsFz6UfYusvYr9VnuhFaFo)

## Nombre de commandes par jour:

requête:

```python
mycursor.execute('''
SELECT count(*),DAY(order_purchase_timestamp)
FROM Orders
GROUP BY DAY(order_purchase_timestamp)
''')

res = mycursor.fetchall()
print("\nNombre de commandes par jours :")
i = 0
for x in res:
  i += 1
  print(i, x)

```



résultat en console pour les 31 jours:



![img](https://lh6.googleusercontent.com/NtkuY4X-KQyg7dhtOYcfR4JM8mdBzoFLQFYJkYUvT0tTQeovw4Zfr9zQd-sNetwlXzKSJIFSv35ae-YWg8egz7cmAltoUSUvznF2Qg_Ha68XGmYZi9swZOn2FAgsNg7MTKVxbrH_)



## Durée moyenne entre la commande et la livraison:

requête:

```python
mycursor.execute('''
SELECT AVG(DATEDIFF(order_estimated_delivery_date, order_purchase_timestamp))
AS avg_duration
FROM Orders
''')
res = mycursor.fetchall()
print("\nDurée moyenne entre la commande et la livraison :")
for x in res:
  print(x)
```



résultat en console:

![img](https://lh4.googleusercontent.com/ZuYB_uQZhuQDIENsZKbxVjgfj6WSJEcSsQBu3-eQRZEh-ixyGVE_NwFUPOAux5L4lk8hB3u7QrD7uLqQJBcdYr8zFf_CG9VJqMN4Kl7AmPsXGSOhvstlRLRc4HwhCOMM3rrWmeYU)



## Nombre de commandes par ville (ville du vendeur):

requête:

```python
mycursor.execute('''
SELECT COUNT(oi.order_id),s.seller_city
FROM Order_items AS oi
INNER JOIN Sellers AS s
ON oi.seller_id = s.seller_id
GROUP BY s.seller_city
ORDER BY s.seller_city
''')

res = mycursor.fetchall()

print("\nNombre de commande par ville (ville du vendeur) :")
for x in res:
  print(x)
```



résultat en console (extrait):

![img](https://lh5.googleusercontent.com/dEp3xY_GvypGt7-Zy6T4Uo9z3ycc_fYlWcHJZ3z24CMrH5wHMrrYZPz_B-4SFxbC_O0_luz6MT0eNLtnMHLw13xh2ZJDShRfXHuhR0-uSgr8HgoFhC4aBPUtDkFvk74blqMK2RiM)



## Prix minimum des commandes:

requête:

```python
mycursor.execute('''
SELECT MIN(payment_value)
FROM Order_payments
''')

res = mycursor.fetchall()
print("\nPrix minimum des commandes:")
for x in res:
  print(x)
```



résultat en console:

![img](https://lh4.googleusercontent.com/Efm_px6w9ViusQaHdPpJ3_Kflm85idJVvH7-QEl2yhPrjvKPoAtnJHRO6M07peXqGIlmG1GOsJNlzcU3jPjyQZz-ENNA3NSLT24uyhWxlKUHeVQiHWQoCDo3dqMHwLBrx9Gs8dVl)





## Prix maximum des commandes:

requête:

```python
mycursor.execute('''
  SELECT MAX(sum)
  FROM(
    SELECT order_id, SUM(payment_value) as sum
    FROM Order_payments
    GROUP BY order_id)
  AS sum_table
''')
```

```python
res = mycursor.fetchall()
print("\nPrix maximum des commandes")
i=0
for x in res:
  i+=1
  print(i,x)

```

résultat en console:

![img](https://lh5.googleusercontent.com/h3I8nxRb9vSrPjCq6cj9mCiZMgiQeQ0msg8BBVGQhVwQTcX102Iz4tbUnGs2ZizF7-gC5NBG0j1z1l2S1Z1CTKwU8o9VyqOYAWUG1Sq9UFkQs4rKgRngWORKCSOlK-vVUarXkUoH)



## Le temps moyen d'une livraison par mois:

requête:

```python
 mycursor.execute('''
            SELECT MONTH(order_purchase_timestamp),AVG(DATEDIFF(order_estimated_delivery_date, order_purchase_timestamp))
            FROM Orders
            GROUP BY MONTH(order_purchase_timestamp)
            ORDER BY MONTH(order_purchase_timestamp)
    ''')
    res = mycursor.fetchall()
    print("\nLe temps moyen d'une livraison par mois")

    for x in res:
        print(x)

```



résultat en console, pour les 12 mois:

![img](https://lh4.googleusercontent.com/ApSlV_esHYgu8OaGHjz9AZopA1kQDmwEw8LRXtILSJRfJVzG-wDiE_Fq0d03NyKYapgEEmLRQvtZZ09MqGuvZTU6vvN7SHNpaOUWQRwKTaTScnhvvOqIAae8JYtVw-okCCuYAYfo)









## INSERT INTO PRODUCTS



requêtes:

```python
import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="marie",
    password="marikiki9283",
    database="olist")
mycursor = mydb.cursor()

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

```



**résultats en console (après 6 itérations):**

**![img](https://lh4.googleusercontent.com/52m1Qju9u-a_NY8DmeOVSKaS4w-3KbEZEmiIFk38Ca2IS4zMgYrEweXE1bhho6RPSoGsgnag7UdJCAoYEPISsu5utzjIQXOr-MpQ5rjYMzgAkzRNo2I9FS0DmnxC0y0n6ZPE8JQ_)**











# **IV - Améliorations**



## **Gestion des doublons**



Certaines tables présentent des doublons, notamment le jeu de données "Géolocation". Les identifier et les supprimer aurait permis d'alléger la table et de la rendre plus exploitable. 



*Permet d’afficher les doublons de la table Geolocation*

```python

SELECT *, COUNT(*) 
FROM Geolocation 
GROUP BY geolocation_lat, geolocation_lng, geolocation_zip_code_prefix, geolocation_city, geolocation_state 
HAVING COUNT(*)>1 
ORDER BY COUNT(*);

```

*Extrait du résultat de la requête d’affichage des lignes avec en dernière colonne le nombre de répétitions:*

![img](https://lh4.googleusercontent.com/XH3A4kXLtrXvZ0TOYqZvCdxfsLQPUP7rgQbLnbXNlae_GgksQuQFdLfTxBdGaSdUHOrjqLSW1rBpEFh4GsNAkzj9bOUHxzdPIvARIiABlk25BNoEyF1pEXO1n-HY5G57OZYEz3mc)



Nous voyons que pour la table Geolocation, il y a 131544 lignes qui sont répétées plusieurs fois, dont certaines jusqu'à 314 fois. Il faudrait donc actualiser la table Geolocation en supprimant tous ces doublons. 

Pandas nous donne accès à un outil permettant de supprimer les doublons. Il faut donc l'utiliser sur la dataframe avant d'importer les données vers MySQL. 

*Suppression des doublons dans la dataframe df_geolocation* 

```python
df_geolocation = df_geolocation.drop_duplicates(subset=['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'])
```



## **Ajout des contraintes**



Il faudrait vérifier l’unicité des clés primaires que l’on veut définir dans les tables, ce sont celles permettant de lier les dataset dans le schéma de BDD du client. Si ces valeurs ne sont pas uniques, il faudrait éliminer les valeurs redondantes.



*Permet d’afficher les zip_code_prefix en doublon de la table Geolocation*

```python
SELECT geolocation_zip_code_prefix 
FROM Geolocation 
GROUP BY geolocation_zip_code_prefix 
HAVING COUNT(geolocation_zip_code_prefix)>1;

```

*Extrait du résultat de la requête d’affichage des doublons de zip_code_prefix*

![img](https://lh6.googleusercontent.com/i2IT4GcHMPtRCOTDlxIADhc6cNCemshlr2o-ltghUow4Cw0RG84qEV_t6rAYiBDGFdC5opMqwnw3ZcmsFCe8hqkf26LvlUulrNgsaFI1iOjA6NergkWcJbVbOkcNLebDC0xoLzUX)



Nous voyons que 17972 zip_code_prefix ont au minimum un doublon, alors qu'ils sont censés être des clés primaires donc uniques. Ceci est à mettre en perspective avec le point précédent. Peut-être qu’en supprimant les lignes en doubles, le zip_code_prefix n’aura plus de doublons. 



Pour ajouter les clefs primaires, on procède comme suit : 

*Ajout de customer_id en temps que clé primaire de la table Customers*

```python

mycursor=mydb.cursor()
#Add customer_id as primary key in table Customers
mycursor.execute("ALTER TABLE Customers ADD PRIMARY KEY (customer_id)")

```

De même, on ajoute les clés secondaires en se servant encore du schéma de BDD du client. 

*Ajout de customer_id en temps que clé étrangère de la table Orders*

```python
mycursor=mydb.cursor()
#Add customer_id as foreign key in table Orders
mycursor.execute("ALTER TABLE Orders ADD FOREIGN KEY (customer_id)")
```

Au final, on a le code suivant pour ajouter les clés. Il nous manquera encore une clé pour relier la table Geolocate aux autres. En effet, le zip_code_prefix seul n'est pas unique et ne peut pas être utilisé comme référence.

*Ajout des clés étrangères et primaires*

```python
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
```



## **Données inutilisables**



Lors des requêtes SQL, nous avons observé que le paiement minimum était de 0 ce qui n’était pas cohérent. On peut chercher les lignes qui posent problème.

*Recherche des lignes dans Order_payments pour lesquelles le prix vaut 0*

```python
WITH paymult AS( SELECT order_id, SUM(payment_value) AS somme
                 FROM Order_payments
                 GROUP BY order_id
               )
SELECT order_id 
FROM paymult
WHERE somme=0;
```

![img](https://lh6.googleusercontent.com/E-P1bDMV68O-Uz36fnLa-Tv_P4IhuFt_81E8hcjd8ixrRmLtJFYs4FPy8QnDjAk3i4_P3-fSasJoiyeXFIECjOMwbS4wxCjufGFOMqUoqf-ES2lS0YTKuw6mXihHlNdlIc4lJRs0)

*Résultat de la requête de données dont le paiement vaut 0*



Nous voyons qu’il y a 3 lignes pour lesquelles il n’y a pas de type de paiement et dont la valeur est 0. Ces order_id ne sont pas dans la table Orders qui regroupe les commandes. On peut en déduire qu’il faudrait supprimer ces lignes qui n’ont pas lieu d’être. 



  On retrouve également d’autres problèmes, comme des suites de chiffres dans les noms de villes, qu’il faudrait également traiter. 



## **Regroupement de données**  



  La base de données est issue de données notamment entrées directement par les utilisateurs dont les villes. Il faudrait donc prendre en compte les fautes de frappe et les différences d’écriture liées notamment aux accents, pour regrouper ensemble les mêmes villes ,pour que le traitement soit plus pertinent. 

*Requête pour afficher l’ensemble des seller_city de la table sellers*

```python
 SELECT seller_city 
 FROM Sellers 
 GROUP BY seller_city 
 ORDER BY seller_city;
```

![img](https://lh4.googleusercontent.com/YA6uFkRCtIIUWPJyP6dBfkk5dCXrdwbkcNxQX3CA_wsgYxiHDTVKhVjiURRe0Q0xOCtvCvyBfW_Zq9kTWUPTSNIYmDgu_uePJCWVVhbARstsxZGkElSqnmHwbGqna4wtj9G3sbbb)

*Extrait du résultat de la requête d’affichage des villes des vendeurs*

Nous voyons que Sao Paulo a 6 dénominations différentes pour ce qui représente la même ville. Il faudrait donc regrouper ces lignes pour n’avoir plus qu’un nom unique pour Sao Paulo. 