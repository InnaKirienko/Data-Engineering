import os

AUTH_TOKEN = os.getenv('AUTH_TOKEN')

if not AUTH_TOKEN:
  print("Помилка: Змінна середовища AUTH_TOKEN не встановлена!")
  exit(1)

# Решта вашого коду, який використовує AUTH_TOKEN