# 🏬 Data Warehouse – Superstore

## 📌 Description du projet
Ce projet consiste à construire un **Data Warehouse complet** basé sur les données du jeu de données **Superstore**.  
L’objectif est de centraliser, transformer et structurer les données pour permettre une **analyse fiable** des ventes, des clients, des produits et de la performance globale du magasin.

Le projet applique les bonnes pratiques d’ingénierie des données :  
✔️ Architecture *Bronze → Silver → Gold*  
✔️ Modèle en **Star Schema**  
✔️ Pipeline ETL (SQL + Python)  
✔️ Chargement automatisé des données nettoyées  
✔️ Prêt pour reporting, dashboards et BI  

---

## 📁 Structure du projet
<img width="273" height="566" alt="image" src="https://github.com/user-attachments/assets/36e72c5d-b18e-46c3-b547-317ca54f81d4" />



---

## 🧱 Architecture de l'entrepôt de données

### 🥉 Bronze Layer
- Importation directe des fichiers CSV  
- Aucun nettoyage  
- Donnée brute telle que fournie  

### 🥈 Silver Layer
- Nettoyage des données  
- Validation des types  
- Standardisation des dates  
- Dédoublonnage  
- Génération des clés (date_key, product_key, customer_key…)  

### 🥇 Gold Layer
- Construction des **tables de dimensions**  
- Construction de la **table de faits Fact_Sales**  
- Mise en place d’un **Star Schema** optimisé pour BI  

---

## ⭐ Modèle Dimensionnel – Star Schema
<img width="1211" height="1265" alt="dw drawio" src="https://github.com/user-attachments/assets/5af13e5e-09b8-4918-9073-0cf7a057da7b" />

### 📘 Tables Dimensions
- Dim_Customers  
- Dim_Products  
- Dim_Dates  
- Dim_Shipping  
- Dim_Locations (optionnel)

### 📗 Table des faits
- Fact_Sales  
incluant :  
- Sales  
- Profit  
- Discount  
- Quantity  
- Foreign keys vers les dimensions  
<img width="1422" height="960" alt="image" src="https://github.com/user-attachments/assets/52179b70-f862-4ba4-8fb3-ad5ca627a2b2" />
<img width="1464" height="952" alt="image" src="https://github.com/user-attachments/assets/ddb73173-851e-4ab9-af28-2470c61b4204" />
<img width="1736" height="955" alt="image" src="https://github.com/user-attachments/assets/a4e825c3-84f1-48b9-ab5d-7bab88c9aeae" />
<img width="1912" height="987" alt="image" src="https://github.com/user-attachments/assets/aadeca33-22e4-4f1f-8845-45c27f529614" />
<img width="1626" height="905" alt="image" src="https://github.com/user-attachments/assets/14df8186-d68c-4fbd-a261-ce89fa1567bf" />
<img width="1615" height="910" alt="image" src="https://github.com/user-attachments/assets/2cd051ca-e405-4ca7-9d0c-b2bc778a4b1d" />





<img width="1612" height="860" alt="image" src="https://github.com/user-attachments/assets/61080809-4321-4c72-9f02-a1e3f9bc4a42" />

---

## ⚙️ Installation & Prérequis

### 🔧 Prérequis
- PostgreSQL  
- Python 3 (si pipeline Python)  
- Git  
- pgAdmin / DBeaver (optionnel)

### 🛠️ Installation
```bash
git clone https://github.com/SalmaTAMMARI12/data-warehouse-superstore.git
cd data-warehouse-superstore
🚀 Exécution du pipeline ETL
1️⃣ Créer les schémas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
3️⃣ Nettoyage & Transformation (Silver)
CALL silver.load_silver();

4️⃣ Construction du Gold Layer
CALL gold.build_dimensions();
CALL gold.build_fact_sales();

📊 Exemples d’analyses
**Total des ventes par année**
SELECT d.year, SUM(f.sales)
FROM gold.fact_sales f
JOIN gold.dim_dates d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;

**Top 10 des produits les plus vendus**
SELECT p.product_name, SUM(f.sales) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_sales DESC
LIMIT 10;

📄 Documentation

Tous les schémas, diagrammes et explications sont disponibles dans :
📁 docs/

