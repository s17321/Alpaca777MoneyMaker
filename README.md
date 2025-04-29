# Alpaca Bot (paper-trading)

Minimalny cel → podłączyć się do konta demo w Alpaca i wykonać pierwsze zlecenie testowe.

## Szybki start
```bash
git clone …
cd alpaca-bot
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # uzupełnij klucze API
python -m src
