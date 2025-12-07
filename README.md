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
data-warehouse-superstore/
├── datasets/ # Fichiers sources (CSV)
├── bronze/ # Données brutes importées
├── silver/ # Données nettoyées et transformées
├── gold/ # Tables finalisées (faits + dimensions)
├── scripts/ # Scripts SQL / Python pour l'ETL
├── docs/ # Diagrammes, schémas, documentation
├── tests/ # Tests éventuels
└── README.md # Documentation du projet


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
Total des ventes par année
SELECT d.year, SUM(f.sales)
FROM gold.fact_sales f
JOIN gold.dim_dates d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;

Top 10 des produits les plus vendus
SELECT p.product_name, SUM(f.sales) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_sales DESC
LIMIT 10;

📄 Documentation

Tous les schémas, diagrammes et explications sont disponibles dans :
📁 docs/

🤝 Contribution

Forker le repo

Créer une branche feature/xxx

Soumettre une Pull Request

📜 Licence

Ce projet est publié sous licence MIT.

👩‍💻 Auteur

Salma Tammari
Étudiante en ingénierie des données – ENSIAS
