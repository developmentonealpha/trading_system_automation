from services.data_lake.app import db
from sqlalchemy import UniqueConstraint, text
from datetime import datetime, timedelta
import pandas as pd

Base = db.Model

class RealTimeData(Base):
    __abstract__ = True  # Prevents SQLAlchemy from creating a table for this class

    symbol = db.Column(db.String(20), nullable=False)
    date = db.Column(db.Date, nullable=False)
    time = db.Column(db.Time, nullable=False)
    open = db.Column(db.Numeric(10, 2))
    high = db.Column(db.Numeric(10, 2))
    low = db.Column(db.Numeric(10, 2))
    close = db.Column(db.Numeric(10, 2))
    volume = db.Column(db.BigInteger)

    @classmethod
    def create_symbol_table(cls, symbol):
        """Creates a new table for a symbol with range partitioning (3-month intervals) and indexing."""
        table_name = f"{symbol.lower()}"  # Use lowercase for consistency

        # Check if the table already exists
        check_table_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        """
        result = db.session.execute(text(check_table_sql)).scalar()

        if not result:
            create_table_sql = f"""
                CREATE TABLE {table_name} (
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    time TIME NOT NULL,
                    open NUMERIC(10,2),
                    high NUMERIC(10,2),
                    low NUMERIC(10,2),
                    close NUMERIC(10,2),
                    volume BIGINT,
                    CONSTRAINT {table_name}_unique UNIQUE (symbol, date, time)
                ) PARTITION BY RANGE (date);
            """
            db.session.execute(text(create_table_sql))
            db.session.commit()

            # Add index on (date, time)
            index_sql = f"CREATE INDEX idx_{table_name}_date_time ON {table_name} (date, time);"
            db.session.execute(text(index_sql))
            db.session.commit()

            # Create initial partition for the current quarter
            cls.create_partition(symbol, datetime.today().date())

        return table_name

    @classmethod
    def create_partition(cls, symbol, start_date):
        """Creates a 3-month partition for the symbol's table if it doesn't already exist."""
        table_name = f"{symbol.lower()}"

        # Calculate partition start and end dates
        month = ((start_date.month - 1) // 3) * 3 + 1  # Start of the quarter
        start_date = start_date.replace(month=month, day=1)  # Ensure start is always the 1st
        if month + 3 <= 12:
            end_date = start_date.replace(month=month + 3)
        else:
            end_date = start_date.replace(year=start_date.year + 1, month=1)

        partition_name = f"{table_name}_{start_date.year}_Q{(start_date.month - 1) // 3 + 1}".lower()

        # Check if partition already exists
        check_partition_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = '{partition_name}'
            );
        """
        result = db.session.execute(text(check_partition_sql)).scalar()

        if not result:
            create_partition_sql = f"""
                CREATE TABLE {partition_name} PARTITION OF {table_name}
                FOR VALUES FROM ('{start_date}') TO ('{end_date}');
            """
            db.session.execute(text(create_partition_sql))
            db.session.commit()

            # Add index on (date, time) for the partition
            index_sql = f"CREATE INDEX idx_{partition_name}_date_time ON {partition_name} (date, time);"
            db.session.execute(text(index_sql))
            db.session.commit()
