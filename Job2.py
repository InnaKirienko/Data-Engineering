import os

AUTH_TOKEN = os.getenv('AUTH_TOKEN')

if not AUTH_TOKEN:
  print("Error: AUTH_TOKEN environment variable not set!")
  exit(1)