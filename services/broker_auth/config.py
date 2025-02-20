# FYERS_CONFIG = {
#     "CLIENT_ID": "3TQOJHLJIT-100",  # Replace with your actual Fyers App client ID
#     "SECRET_KEY": "OVRKLX8EM0",  # Replace with your actual Fyers App secret key
#     "REDIRECT_URI": "https://127.0.0.1:8000/",  # Your redirect URI (same as set in Fyers API dashboard)
#     "FY_ID": "YM28412",  # Your Fyers ID
#     "TOTP_KEY": "7GZ5OQCSYZHUJIO6ZYGG3F3XT2V3QHT7",  # TOTP secret generated when enabling 2FA
#     "PIN": "2000"  # Your Fyers trading PIN
# }
# config.py

FYERS_CONFIG = {
    "CLIENT_ID": "3TQOJHLJIT-100",
    "SECRET_KEY": "OVRKLX8EM0",
    "FY_ID": "YM28412",
    "TOTP_KEY": "7GZ5OQCSYZHUJIO6ZYGG3F3XT2V3QHT7",
    "PIN": "2000",
    "REDIRECT_URI": "https://127.0.0.1:8000/"
}


BINANCE_CONFIG = {
    "API_KEY": "LYEu4haYVMKq6eLwZRL6abbaM3MYoHQnI9xGmF2NgIKG5QQIb0uFl3hrxiPTKext",
    "API_SECRET": "UuXHuMrJ7mZiyDuQCNsbKOAA5y72uB6tXVAHYwa3iEKXvNzOIW6SkbvtMVUBvha1",  # Required for HMAC
    "PRIVATE_KEY": None,  # Use None if you're not using RSA keys
    "PRIVATE_KEY_PASS": None  # Use None if no password is required for the private key
}