# COMPTE RENDU HADOOP TP4

### MOHAMED DHIA KASSAB GLSI3TP2

---

## Activité 1 : Comptage des mots

### Code du Mapper :

```python
import sys

for line in sys.stdin:
    # Supprimer les espaces et diviser en mots
    words = line.strip().split()

    # Pour chaque mot, émettre le couple clé-valeur
    for word in words:
        print(f"{word}\t1")
```

### Code du Reducer :

```python
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    word, count = line.strip().split('\t')
    count = int(count)

    if current_word == word:
        current_count += count
    else:
        if current_word:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

if current_word:
    print(f"{current_word}\t{current_count}")
```

### Capture d'écran :
![Image 1](https://github.com/user-attachments/assets/064d211a-050e-48e3-a26e-78af07cdf299)

---

## Activité 2 : Mapper et Reducer de base

### Code du Mapper :

```python
import sys

for line in sys.stdin:
    print(f"{line.strip()}\t1")
```

### Code du Reducer :

```python
import sys
current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()  # Supprimer les espaces inutiles
    key, count = line.split("\t", 1)
    
    try:
        count = int(count)  # Convertir la valeur en entier
    except ValueError:
        # Ignorer les lignes mal formatées
        continue

    if current_key == key:
        current_count += count  # Ajouter au compteur courant
    else:
        if current_key is not None:
            # Imprimer la clé précédente et son total
            print(f"{current_key}\t{current_count}")
        current_key = key
        if current_key == key:
            print(f"{current_key}\t{current_count}")
```

### Capture d'écran :
![Image 2](https://github.com/user-attachments/assets/26c759fc-1f66-4784-8862-e8180fb7af6f)

---

## Activité 3 : Comptage des domaines d'URL

### Code du Mapper :

```python
import sys
from urllib.parse import urlparse

for line in sys.stdin:
    domain = urlparse(line.strip()).netloc
    print(f"{domain}\t1")
```

### Code du Reducer :

```python
import sys
from collections import defaultdict

counts = defaultdict(int)

for line in sys.stdin:
    length, count = line.strip().split('\t')
    counts[length] += int(count)

for length, count in counts.items():
    print(f"{length}\t{count}")
```

### Capture d'écran :
![Image 3](https://github.com/user-attachments/assets/79b9d0c2-b1fd-47ab-b5f7-a4aeb35e4409)

---

## Activité 4 : Comptage des hashtags

### Code du Mapper :

```python
import sys
import re

for line in sys.stdin:
    hashtags = re.findall(r"#(\w+)", line)
    for hashtag in hashtags:
        print(f"{hashtag}\t1")
```

### Code du Reducer :

```python
import sys
from collections import defaultdict

counts = defaultdict(int)

for line in sys.stdin:
    length, count = line.strip().split('\t')
    counts[length] += int(count)

for length, count in counts.items():
    print(f"{length}\t{count}")
```

### Capture d'écran :
![Image 4](https://github.com/user-attachments/assets/81f64621-2245-42d4-a525-9a2f97d527e8)

---

## Activité 5 : Trier les prix par catégorie

### Code du Mapper :

```python
import sys

for line in sys.stdin:
    category, price = line.strip().split('\t')
    print(f"{category}\t{price}")
```

### Code du Reducer :

```python
import sys
from collections import defaultdict

data = defaultdict(list)

for line in sys.stdin:
    category, price = line.strip().split('\t')
    data[category].append(float(price))

for category, prices in data.items():
    for price in sorted(prices):
        print(f"{category}\t{price}")
```

### Capture d'écran :
![Image 5](https://github.com/user-attachments/assets/f71c43da-6154-43c8-b627-702d44da0dab)

---

## Activité 6 : Données de température par saison

### Code du Mapper :

```python
import sys
from datetime import datetime

for line in sys.stdin:
    date, temp = line.strip().split('\t')
    month = datetime.strptime(date, "%Y-%m-%d").month
    if month in [12, 1, 2]:
        season = "Winter"
    elif month in [3, 4, 5]:
        season = "Spring"
    elif month in [6, 7, 8]:
        season = "Summer"
    else:
        season = "Fall"
    print(f"{season}\t{temp}")
```

### Code du Reducer :

```python
import sys

current_season = None
temp_sum = 0
temp_count = 0
temp_min = float('inf')
temp_max = float('-inf')

def print_season_summary(season, temp_sum, temp_count, temp_min, temp_max):
    """Imprimer les statistiques pour une saison donnée"""
    if temp_count > 0:
        avg_temp = temp_sum / temp_count
        print(f"{season}\tMoyenne: {avg_temp:.2f}\tMin: {temp_min}\tMax: {temp_max}")

for line in sys.stdin:
    line = line.strip()

    try:
        # Analyser la ligne : saison et température
        season, temp = line.split("\t")
        temp = float(temp)
    except ValueError:
        # Ignorer les lignes mal formatées
        continue

    if current_season == season:
        temp_sum += temp
        temp_count += 1
        temp_min = min(temp_min, temp)
        temp_max = max(temp_max, temp)
    else:
        if current_season is not None:
            print_season_summary(current_season, temp_sum, temp_count, temp_min, temp_max)

        current_season = season
        temp_sum = temp
        temp_count = 1
        temp_min = temp
        temp_max = temp

if current_season is not None:
    print_season_summary(current_season, temp_sum, temp_count, temp_min, temp_max)
```

### Capture d'écran :
![Image 6](https://github.com/user-attachments/assets/d3eedfa4-d671-41ef-a1d0-ef5cb6e9da50)

---

## Activité 7 : Données de ventes par employé

### Code du Mapper :

```python
import sys

# Lecture de chaque ligne du fichier d'entrée
for line in sys.stdin:
    # Supprimer les espaces inutiles
    line = line.strip()
    # Découper la ligne en colonnes
    parts = line.split("\t")

    if len(parts) == 4:
        emp_id, name, month, sales = parts
        try:
            sales = int(sales)
            # Émettre le résultat : clé = emp_id, valeur = montant des ventes
            print(f"{emp_id}\t{sales}")
        except ValueError:
            # Si la conversion échoue, on ignore la ligne
            continue
```

### Code du Reducer :

```python
import sys

current_emp_id = None
current_total_sales = 0

for line in sys.stdin:
    line = line.strip()
    emp_id, sales = line.split("\t")

    try:
        sales = int(sales)
    except ValueError:
        continue

    if current_emp_id and emp_id != current_emp_id:
        print(f"{current_emp_id}\t{current_total_sales}")
        current_total_sales = 0 

    current_emp_id = emp_id
    current_total_sales += sales

if current_emp_id:
    print(f"{current_emp_id}\t{current_total_sales}")
```

### Capture d'écran :
![Image 7](https://github.com/user-attachments/assets/f88f38a1-e4e0-4c59-990d-f10d2c7ecb2b)
