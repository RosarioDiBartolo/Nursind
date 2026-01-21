# Cartellino Parser

Parser for "Cartellino mensile configurabile" PDFs (Azienda Ospedaliera di Perugia / INSIEL).

## Usage

Install deps:

```bash
python -m pip install -e .[dev]
```

Run the CLI:

```bash
python -m cartellino_parser.cli parse --input documents --out output
```

Outputs per PDF:
- `*.days.csv`
- `*.pairs.csv`
- `*.totals.json`
- `*.report.json`

Programmatic usage:

```python
from cartellino_parser import parse_pdf

parsed = parse_pdf("documents/Cartellino mensile-2022-07.pdf")
print(parsed.meta)
print(parsed.days_df.head())
print(parsed.pairs_df.head())
print(parsed.totals)
print(parsed.validation)
```
