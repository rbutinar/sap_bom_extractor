# SAP BOM Extractor with PySpark

This project demonstrates how to extract and transform SAP Bill of Materials (BOM) data using PySpark, without relying on SAP-specific functions. It is designed to run locally for development and testing.

## Setup

1. **Create a virtual environment (recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Running Locally

- You can run scripts or Jupyter notebooks directly. PySpark will use local mode.
- Example to start Jupyter:
  ```bash
  jupyter notebook
  ```
- Example to run a script:
  ```bash
  python your_script.py
  ```

## Project Structure

- `notebooks/` - Jupyter notebooks for prototyping and exploration
- `src/` - Python modules for reusable logic
- `data/` - Sample input data (CSV, parquet, etc.)
- `requirements.txt` - Python dependencies
- `README.md` - Project documentation

## Next Steps
- Refactor the initial notebooks into scripts/modules for maintainability.
- Add sample data and transformation logic.
- Expand the pipeline iteratively.

---

Feel free to reach out for help with the next steps or running your first PySpark job!
