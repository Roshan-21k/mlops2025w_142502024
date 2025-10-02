import os
import math
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError, BulkWriteError
from pathlib import Path
from dotenv import load_dotenv

# ----------------------------
# 0) Config / .env
# ----------------------------
# Force .env to override any existing env variable values so
# a previously exported MONGO_URI won't leak into this run.
load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=True)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/retail")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI not set (check your .env or environment).")

def _mask(uri: str) -> str:
    # mask password for logs
    if "://" not in uri:
        return uri
    scheme, rest = uri.split("://", 1)
    if "@" not in rest or ":" not in rest.split("@", 1)[0]:
        return uri
    creds, tail = rest.split("@", 1)
    user = creds.split(":", 1)[0]
    return f"{scheme}://{user}:***@{tail}"

print(f"Connecting with URI: {_mask(MONGO_URI)}")

# ----------------------------
# 1) Connection (pooled client)
# ----------------------------
def get_client():
    """
    Pooled MongoDB client.
    """
    return MongoClient(
        MONGO_URI,
        maxPoolSize=50,
        minPoolSize=0,
        serverSelectionTimeoutMS=7000,
        connectTimeoutMS=7000,
        socketTimeoutMS=20000,
        retryWrites=True,
    )

# ----------------------------
# 2) Utilities
# ----------------------------
def parse_invoice_date(s):
    """
    The dataset often has '01-12-2010 08:26' (DD-MM-YYYY HH:MM).
    We try a couple formats and fall back to pandas.
    """
    if pd.isna(s):
        return None
    txt = str(s).strip()
    for fmt in ("%d-%m-%Y %H:%M", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(txt, fmt)
        except Exception:
            pass
    try:
        return pd.to_datetime(txt, dayfirst=True, errors="coerce").to_pydatetime()
    except Exception:
        return None

def is_cancellation(invoice_no: str) -> bool:
    return str(invoice_no).startswith("C")

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def safe_int(x):
    try:
        return int(float(x))
    except Exception:
        return None

# ----------------------------
# 3) Build documents
# ----------------------------
def build_txn_doc(group: pd.DataFrame):
    """
    Build one transaction-centric invoice document from all rows of an invoice.
    """
    first = group.iloc[0]
    invoice_no = str(first["InvoiceNo"])
    customer_id = safe_int(first.get("CustomerID"))
    invoice_date = parse_invoice_date(first.get("InvoiceDate"))
    country = str(first.get("Country")) if "Country" in group.columns else None

    items = []
    for _, r in group.iterrows():
        items.append({
            "stock_code": str(r["StockCode"]),
            "description": (None if pd.isna(r.get("Description")) else str(r.get("Description"))),
            "unit_price": safe_float(r.get("UnitPrice")),
            "quantity": safe_int(r.get("Quantity"))
        })

    return {
        "_id": invoice_no,
        "invoice_date": invoice_date,
        "customer": {"id": customer_id, "country": country},
        "items": items,
        "is_cancellation": is_cancellation(invoice_no)
    }

def customer_upsert_ops(df):
    ops = []
    for invoice_no, g in df.groupby("InvoiceNo"):
        first = g.iloc[0]
        invoice_date = parse_invoice_date(first.get("InvoiceDate"))
        cust_id = safe_int(first.get("CustomerID"))
        country = str(first.get("Country")) if "Country" in g.columns else None

        items = []
        for _, r in g.iterrows():
            items.append({
                "stock_code": str(r["StockCode"]),
                "quantity": safe_int(r.get("Quantity")),
                "unit_price": safe_float(r.get("UnitPrice"))
            })

        invoice_doc = {
            "invoice_no": str(invoice_no),
            "invoice_date": invoice_date,
            "items": items,
            "is_cancellation": is_cancellation(str(invoice_no))
        }

        ops.append(
            UpdateOne(
                {"_id": cust_id},
                {
                    # only set country on first insert; don't pre-create 'invoices'
                    "$setOnInsert": {"country": country},
                    # push will create the array if it doesn't exist
                    "$push": {"invoices": invoice_doc}
                    # If you want to avoid exact duplicates on reruns, use:
                    # "$addToSet": {"invoices": invoice_doc}
                },
                upsert=True
            )
        )
    return ops


# ----------------------------
# 4) Loader
# ----------------------------
def load_to_mongo(path, nrows=None, chunksize=None):
    """
    Load data into both collections.
    - If nrows is set, we read that many rows quickly (good for assignment minimum).
    - If chunksize is set, we stream in chunks (good for full dataset).
    """
    client = get_client()
    try:
        # Use DB from URI if present, otherwise 'retail'
        db = client.get_default_database()
        if db is None:
            db = client["retail"]
        invoices_txn = db["invoices_txn"]
        customers_centric = db["customers_centric"]

        # Indexes (idempotent)
        invoices_txn.create_index([("customer.id", ASCENDING)])
        invoices_txn.create_index([("invoice_date", ASCENDING)])
        customers_centric.create_index([("country", ASCENDING)])

        def process_frame(df: pd.DataFrame):
            # Basic cleaning
            needed = ["InvoiceNo", "StockCode", "Quantity"]
            df = df.dropna(subset=[c for c in needed if c in df.columns])
            if "CustomerID" in df.columns:
                df = df.dropna(subset=["CustomerID"])
            else:
                # If CustomerID column is completely missing, skip (dataset variant)
                return

            # Build docs
            txn_docs = [build_txn_doc(g) for _, g in df.groupby("InvoiceNo")]
            ops = customer_upsert_ops(df)

            # Bulk insert with duplicate handling
            if txn_docs:
                try:
                    invoices_txn.insert_many(txn_docs, ordered=False)
                except BulkWriteError as bwe:
                    # ignore duplicate key errors when rerunning
                    errs = bwe.details.get("writeErrors", [])
                    if not all(e.get("code") == 11000 for e in errs):
                        raise

            if ops:
                try:
                    customers_centric.bulk_write(ops, ordered=False)
                except BulkWriteError as bwe:
                    errs = bwe.details.get("writeErrors", [])
                    if not all(e.get("code") == 11000 for e in errs):
                        raise

        print(f"Loading from: {path}")
        if path.lower().endswith(".csv"):
            if chunksize:
                for i, ch in enumerate(pd.read_csv(path, chunksize=chunksize)):
                    print(f"  Processing chunk {i+1}...")
                    process_frame(ch)
            else:
                df = pd.read_csv(path, nrows=nrows)
                process_frame(df)
        else:
            # Excel
            df = pd.read_excel(path, nrows=nrows)
            process_frame(df)

        print("✅ Load completed.")
    except ServerSelectionTimeoutError:
        print("❌ ERROR: Could not reach MongoDB server. Check your MONGO_URI, firewall/VPN, or internet.")
    except PyMongoError as e:
        print(f"❌ Mongo error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        client.close()

# ----------------------------
# 5) CRUD helpers (for Q3)
# ----------------------------
def create_invoice(db, invoice_doc):
    try:
        db.invoices_txn.insert_one(invoice_doc)
    except PyMongoError as e:
        print(f"Create failed: {e}")

def read_invoices_by_customer(db, customer_id, limit=5):
    try:
        return list(db.invoices_txn.find({"customer.id": customer_id}).sort("invoice_date", 1).limit(limit))
    except PyMongoError as e:
        print(f"Read failed: {e}")
        return []

def update_item_quantity(db, invoice_no, stock_code, new_qty):
    try:
        db.invoices_txn.update_one(
            {"_id": invoice_no, "items.stock_code": stock_code},
            {"$set": {"items.$.quantity": new_qty}}
        )
    except PyMongoError as e:
        print(f"Update failed: {e}")

def delete_invoice(db, invoice_no):
    try:
        db.invoices_txn.delete_one({"_id": invoice_no})
        db.customers_centric.update_many({}, {"$pull": {"invoices": {"invoice_no": invoice_no}}})
    except PyMongoError as e:
        print(f"Delete failed: {e}")

# ----------------------------
# 6) Main
# ----------------------------
if __name__ == "__main__":
    # Use CSV or Excel; put the file next to this script.
    # Small, fast run to meet assignment requirement (>=1000 rows):
    DATA_PATH = "Online Retail.xlsx"  # or "Online Retail.csv"

    if os.path.exists(DATA_PATH):
        load_to_mongo(DATA_PATH, nrows=20000)  # change nrows as you like
        # For the full dataset later, prefer: load_to_mongo(DATA_PATH, chunksize=50000)
    else:
        print(f"File not found: {DATA_PATH}. Put your CSV/Excel next to this script.")
