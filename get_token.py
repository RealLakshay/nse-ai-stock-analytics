from kiteconnect import KiteConnect

api_key = "paste_your_api_key_here"
api_secret = "paste_your_api_secret_here"

kite = KiteConnect(api_key=api_key)

# Step 1: Get login URL
login_url = kite.login_url()
print("Open this URL in browser:")
print(login_url)
print()
print("After login, paste the full redirect URL here:")
redirect_url = input("Paste URL: ")

# Step 2: Extract request token from URL
request_token = redirect_url.split("request_token=")[1].split("&")[0]
print("Request token:", request_token)

# Step 3: Generate access token
data = kite.generate_session(request_token, api_secret=api_secret)
access_token = data["access_token"]

print()
print("YOUR ACCESS TOKEN:")
print(access_token)
print()
print("Copy this into your .env file as KITE_ACCESS_TOKEN=", access_token)