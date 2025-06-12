from whatsapp_api_client_python import API


greenAPI = API.GreenAPI(
    "7105258380", "cc8939e52e844f83b2cf3bc73e0bc8ae3aa69365a28d4753aa"
)

response = greenAPI.sending.sendMessage("84796781365@c.us", "test message 2")

print(response.data)