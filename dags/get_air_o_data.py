import requests


API_KEY = "9302d96b-c689-4b87-93cc-05e1a48e9aa3"
city = 'Bangkok'
state = 'Bangkok'
country = 'Thailand'

url = f"http://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={API_KEY}"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()
print(data)

