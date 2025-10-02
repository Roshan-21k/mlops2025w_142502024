import os, time, random
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from statistics import mean

load_dotenv(Path("mongo_load.py").with_name(".env"), override=True)
client = MongoClient(os.environ["MONGO_URI"])

db = client.get_default_database()
if db is None:
    db = client["retail"]
tx = db.invoices_txn
cc = db.customers_centric

# ensure indexes exist (idempotent)
tx.create_index([("customer.id", ASCENDING)])
tx.create_index([("invoice_date", ASCENDING)])
cc.create_index([("country", ASCENDING)])

def timeit(fn, iters=5):
    times=[]
    for _ in range(iters):
        t0=time.perf_counter(); fn(); times.append((time.perf_counter()-t0)*1000)
    return round(mean(times),2)

# ---- pick existing ids for fair tests ----
any_tx = tx.find_one({}, {"_id":1, "customer.id":1, "items.stock_code":1})
invoice_id = any_tx["_id"]
cust_id = any_tx["customer"]["id"]
stock = any_tx["items"][0]["stock_code"]

# ---------- TRANSACTION-CENTRIC CRUD ----------
def tx_create():
    tx.insert_one({
        "_id": "TEST_TXN_1",
        "invoice_date": any_tx.get("invoice_date"),
        "customer": {"id": 99998, "country": "Testland"},
        "items": [{"stock_code": "T1", "description": "Test Item", "unit_price": 1.11, "quantity": 2}],
        "is_cancellation": False
    })
    tx.delete_one({"_id": "TEST_TXN_1"})  # cleanup

def tx_read():
    list(tx.find({"customer.id": cust_id}).limit(200))  # common query

def tx_update():
    tx.update_one({"_id": invoice_id, "items.stock_code": stock},
                  {"$set": {"items.$.quantity": 9}})

def tx_delete():
    tx.insert_one({
        "_id": "TEST_TXN_2",
        "invoice_date": any_tx.get("invoice_date"),
        "customer": {"id": 99997, "country": "Testland"},
        "items": [{"stock_code": "T2", "description": "Tmp", "unit_price": 1.0, "quantity": 1}],
        "is_cancellation": False
    })
    tx.delete_one({"_id": "TEST_TXN_2"})

# ---------- CUSTOMER-CENTRIC CRUD ----------
def cc_create():
    cc.update_one({"_id": 99999},
        {"$setOnInsert": {"country":"Testland"},
         "$addToSet": {"invoices": {
            "invoice_no":"TEST_CC_1","invoice_date": any_tx.get("invoice_date"),
            "items":[{"stock_code":"T1","unit_price":1.11,"quantity":2}],
            "is_cancellation": False}}},
        upsert=True)
    cc.update_one({"_id": 99999}, {"$pull": {"invoices": {"invoice_no":"TEST_CC_1"}}})

def cc_read():
    # read whole customer history (typical)
    cc.find_one({"_id": cust_id})

def cc_update():
    # set quantity on a specific invoice inside array
    cc.update_one(
        {"_id": cust_id, "invoices.invoice_no": invoice_id},
        {"$set": {"invoices.$[inv].items.0.quantity": 8}},
        array_filters=[{"inv.invoice_no": invoice_id}]
    )

def cc_delete():
    cc.update_one({"_id": cust_id}, {"$pull": {"invoices": {"invoice_no": invoice_id}}})

tests = [
    ("TX Create", tx_create),
    ("TX Read (by customer)", tx_read),
    ("TX Update (line qty)", tx_update),
    ("TX Delete", tx_delete),
    ("CC Create", cc_create),
    ("CC Read (customer doc)", cc_read),
    ("CC Update (nested)", cc_update),
    ("CC Delete (pull invoice)", cc_delete),
]

print("IDs used -> invoice:", invoice_id, "customer:", cust_id, "stock:", stock)
for name, fn in tests:
    try:
        ms = timeit(fn, iters=5)
        print(f"{name:26s}: ~{ms} ms")
    except Exception as e:
        print(f"{name:26s}: ERROR -> {e}")

# PROOF with explain (query plan + docs examined)
print("\nExplain (TX read by customer):")
print(tx.find({"customer.id": cust_id}).explain()["executionStats"])

print("\nExplain (CC read by _id):")
print(cc.find({"_id": cust_id}).explain()["executionStats"])
