import asyncio
from rustplus import RustSocket

async def main():
    print("Rust+ Token Grabber Helper")
    print("----------------------------")
    print("This script helps you find your token if you have previously paired with the server.")
    print("Requires `rustplus` library installed and configuration/fcm credentials if strict.")
    print("Actually, the easiest way is to use the CLI from the library.")
    print("Run: `python -m rustplus.cli` (if available) or check the official repo.")
    
    # Actually, without FCM credentials (which are complex to set up), you can't just "grab" it easily 
    # unless you are simulating a device registration or intercepting.
    # The most common user-friendly way is using an online tool like:
    # https://rustplus.bot-hosting.net/ (Example, or similar widely used ones)
    # Or 'RustPlusBot' discord bot DM.
    
    print("\nRECOMMENDED METHOD:")
    print("1. Go to a trusted Rust+ Token Grabber website (e.g. repl.it scripts commonly found on Google).")
    print("2. Login with your Steam account.")
    print("3. It will list your paired servers and tokens.")
    print("4. Copy the integer token.")

if __name__ == "__main__":
    asyncio.run(main())
