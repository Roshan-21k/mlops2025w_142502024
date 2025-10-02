def main():
    import pandas as pd
    import sqlite3

    df = pd.read_excel("Online Retail.xlsx", nrows=10000)
    print(df.head())

    df = df.dropna(subset=["InvoiceNo", "StockCode", "CustomerID"])

    customers = df[["CustomerID", "Country"]].drop_duplicates()
    products = df[["StockCode", "Description", "UnitPrice"]].drop_duplicates()
    invoices = df[["InvoiceNo", "InvoiceDate", "CustomerID"]].drop_duplicates()
    invoice_items = df[["InvoiceNo", "StockCode", "Quantity"]]

    conn = sqlite3.connect("retail.db")
    customers.to_sql("Customers", conn, if_exists="replace", index=False)
    products.to_sql("Products", conn, if_exists="replace", index=False)
    invoices.to_sql("Invoices", conn, if_exists="replace", index=False)
    invoice_items.to_sql("InvoiceItems", conn, if_exists="replace", index=False)

    conn.close()
    print("Data normalized into SQLite DB!")
if __name__ == "__main__":
    main()
