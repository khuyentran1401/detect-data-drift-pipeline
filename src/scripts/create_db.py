from pathlib import Path
import sys

# Get the absolute path to the current directory 
project_root = Path(__file__).resolve().parent.parent.parent

# Add the project root directory to the Python path
sys.path.append(str(project_root))

from typing import Text
from sqlalchemy import create_engine
from src.utils.models import Base

if __name__ == "__main__":
    DATABASE_URI: Text = "postgresql://khuyentran:123456@localhost:5432/monitoring_db"
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
