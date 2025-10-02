# Assignment 3 - Q3 Instructions

## Setup
```bash
cd Assignments/"Assignment 4"
uv sync
```

If you are in GitBash or WSL:
```bash
source .venv/Scripts/activate
```

If you’re in PowerShell:
```powershell
.venv\Scripts\Activate.ps1
```

If you’re in Command Prompt (cmd.exe):
```cmd
.venv\Scripts\activate.bat
```

**RECOMMENDED**: Please use GitBash or Command Prompt

This will install all dependencies into the virtual environment and activate it.

---

## To check Q1-Q3 parts, please run the following commands in order:

### Q1 - SQL (Relational Schema in SQLite)
```bash
uv run python question-1.py
```


**What this does:**
- Creates a 2NF schema in retail.db.
- Loads at least 1000 records into the database.


---

### Q2 – MongoDB (Transaction-Centric & Customer-Centric)
```bash
uv run python mongo_load.py
```

**What this does:**
- Loads the dataset into MongoDB Atlas/local.
- Creates two collections:
    - invoices_txn → Transaction-centric (one document per invoice).
    - customers_centric → Customer-centric (one document per customer).

---

### Q3 – CRUD + Benchmarking
```bash
uv run python benchmark_q3.py
```

**What this does:**
- Performs CRUD operations on both models.
- Prints timing results for Create, Read, Update, and Delete.
- Shows execution plans with .explain() for performance analysis.
