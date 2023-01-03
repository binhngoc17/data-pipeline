## Setup

```
pip install -r requirements.txt
```

## Run scipt merge data

```
python main.py
```

## Run Flask server

```
flask --app app run
```

### API document

- Api filter by hotel ids

```
GET /hotels?hotel_ids=SjyX,f8c9
```

- Api filter by destination id

```
GET /destinations/5432
```

## Test

```
python -m coverage run -m unittest test -v
```

### Report coverage testing

```
python -m coverage report
```

### Generate the coverage report in HTML format

```
python -m coverage html
```
