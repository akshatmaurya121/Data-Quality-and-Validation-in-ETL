/* ================================================================
   DATA QUALITY AND VALIDATION ASSIGNMENT
   ================================================================ */

/* Ans1: - Data Quality refers to the accuracy, completeness, consistency, validity, and reliability of data flowing through ETL pipelines.
- It is more than cleaning because:
- Cleaning only removes errors (e.g., nulls, duplicates).
- Quality ensures data is fit for business use, aligned with rules, and trustworthy for decision-making.
- It involves validation, referential integrity, business rule enforcement, and monitoring.

Ans2: - Dashboards rely on aggregated and transformed data.
- Poor quality (duplicates, missing values, wrong mappings) causes:
- Inflated KPIs (e.g., duplicate transactions increase revenue falsely).
- Incorrect trends (missing dates distort time-series analysis).
- Misallocation of resources (wrong city/customer mapping misguides strategy).
Example: In the dataset, Rahul Mehta’s duplicate transactions would overstate Mumbai sales.

Ans3: - Duplicate data: Multiple records representing the same entity/transaction.
- Causes in ETL:
- Multiple source systems → Same transaction loaded twice.
- Improper join logic → Cartesian joins create duplicates.
- Re-run of ETL jobs without deduplication → Data appended repeatedly.

Ans4: Types: Exact- All fields identical
Partial-Some fields match, others differ
Fuzzy- similar match but not identical.(spelling/format variations)

Ans5: - During transformation:
- Prevents bad data from entering the warehouse.
- Saves storage and processing costs.
- Ensures downstream dashboards are accurate.
- After loading → Errors already propagated, harder to fix.

Ans6: - Business rules enforce domain-specific logic.
- Example:
- Rule: Txn_Amount = Quantity × Unit Price.
- In dataset, Neha Singh’s Quantity is Null but Txn_Amount is 2500 → violates rule.
- Such rules ensure data reflects real-world transactions.

-- Creating Tables to run queries

CREATE TABLE IF NOT EXISTS Customers_Master (
    CustomerID VARCHAR(10) PRIMARY KEY,
    CustomerName VARCHAR(50),
    City VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Sales_Transactions (
    Txn_ID INT PRIMARY KEY,
    Customer_ID VARCHAR(10),
    Customer_Name VARCHAR(50),
    Product_ID VARCHAR(10),
    Quantity INT,
    Txn_Amount INT,
    Txn_Date DATE,
    City VARCHAR(50)
);

-- Data Inserting 

INSERT IGNORE INTO Customers_Master VALUES ('C101','Rahul Mehta','Mumbai'), ('C102','Anjali Rao','Bengaluru'), ('C103','Suresh lyer','Chennai'), ('C104','Neha Singh','Delhi');

INSERT IGNORE INTO Sales_Transactions VALUES 
(201,'C101','Rahul Mehta','P11',2,4000,'2025-12-01','Mumbai'),
(202,'C102','Anjali Rao','P12',1,1500,'2025-12-01','Bengaluru'),
(203,'C101','Rahul Mehta','P11',2,4000,'2025-12-01','Mumbai'),
(204,'C103','Suresh lyer','P13',3,6000,'2025-12-02','Chennai'),
(205,'C104','Neha Singh','P14',NULL,2500,'2025-12-02','Delhi'),
(206,'C105','N/A','P15',1,NULL,'2025-12-03','Pune'),
(207,'C106','Amit Verma','P16',1,1800,NULL,'Pune'),
(208,'C101','Rahul Mehta','P11',2,4000,'2025-12-01','Mumbai');

-- ================================================================
-- ANSWER 7: Duplicate detection query [cite: 17, 18]
-- ================================================================

SELECT 
    Customer_ID, Product_ID, Txn_Date, Txn_Amount, 
    COUNT(*) as Duplicate_Count
FROM Sales_Transactions
GROUP BY Customer_ID, Product_ID, Txn_Date, Txn_Amount
HAVING COUNT(*) > 1;

This will flag Rahul Mehta’s repeated transactions (C101, P11, 2025-12-01, 4000).
-- ================================================================
-- ANSWER 8: Referential Integrity violation query 
-- ================================================================

SELECT DISTINCT st.Customer_ID
FROM Sales_Transactions st
LEFT JOIN Customers_Master cm ON st.Customer_ID = cm.CustomerID
WHERE cm.CustomerID IS NULL;

Violations in dataset:
- C105 (Customer Name = N/A, not in master).
- C106 (Amit Verma, not in master).