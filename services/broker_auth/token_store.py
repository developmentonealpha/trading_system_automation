import sqlite3

class TokenStore:
    def __init__(self, service_name, db_file="tokens.db"):
        """Initialize the TokenStore with local SQLite database file."""
        self.service_name = service_name
        self.db_file = db_file
        self._create_table()

    def _create_table(self):
        """Create a local SQLite table for storing tokens if it doesn't exist."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                service_name TEXT PRIMARY KEY,
                access_token TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()

    def save_token(self, token):
        """Save or update the access token in the local database."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO tokens (service_name, access_token, created_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(service_name) DO UPDATE SET 
                access_token = excluded.access_token,
                created_at = CURRENT_TIMESTAMP
        """, (self.service_name, token))
        conn.commit()
        cursor.close()
        conn.close()

    def load_token(self):
        """Load the access token from the local database."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT access_token FROM tokens WHERE service_name = ?", (self.service_name,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else None
