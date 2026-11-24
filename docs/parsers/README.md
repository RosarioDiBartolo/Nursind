# Guida ai parser delle timbrature

Apri [index.html](index.html) in un browser. La guida funziona offline e usa i
[documenti sintetici di test](../../tests/fixtures/parsers/). Ogni parser ha una
cartella con il suo `guide.json` e i file `*.case.json`. Dove presenti, i
campioni TXT preesistenti verificano anche la scelta del parser.

La pagina ricostruisce il testo e, nei casi tabellari, le posizioni orizzontali
che il parser riceve dopo `extract_documents`. Il verde indica una posizione
riportata dal codice. Quando il fallback conosce soltanto la riga, la vista
segnala il candidato sulla riga intera. Le note sui periodi e sugli scarti
spiegano le regole, mentre gli eventi e le decisioni vengono dall'esecuzione della
pipeline sui campioni. La logica condivisa per il nominativo è quella usata dal
mapping e viene eseguita separatamente dall'estrazione degli eventi.

| Parser | Casi documentali |
| --- | --- |
| Cartellino classico | Rilevazione compatta, riepilogo, uscita isolata, riposo, nome strutturato |
| Cartellino OCR | Testo degradato, separatori, uscita con `rec`, frammenti nelle colonne, totale esterno |
| Cartellino Unico | Prefissi E/U, allineamento testuale, dodici colonne, notte, ultima coppia, uscita isolata |
| Situazione mensile | Titolo OCR, correzione orari, conflitto col calendario, periodo assente, nome strutturato e falso candidato |
| Timbrature Web | Timbri E/U, 24:00, ferie, orari invalidi, identità ambigua |
| WORKFLOW mensile | Sei colonne, slot mancanti, notte, layout assente, periodo assente |

Per aggiungere un caso, copia un `*.case.json` nella cartella del parser. In
`lines` ogni riga è una stringa oppure una lista di parole `[testo, x0, larghezza]`
in punti su una pagina da 595 punti. Le chiavi in `notes` sono i numeri di riga a
partire da 1. Usa `period_lines` per evidenziare il titolo o le righe che mostrano il periodo. I valori in `expected` sono le aspettative del test, verificate
contro gli eventi emessi, i giorni riconosciuti, l'identità e dove previsto gli
slot e la decisione della pagina. I test coprono l'ingresso dei parser dopo
l'estrazione documentale. Le prove su PDF e Tesseract vivono nella suite
[`test_document_ocr.py`](../../tests/test_document_ocr.py).

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_parser_documents.py
.\.venv\Scripts\python.exe src/scripts/tools/build_parser_catalog.py
.\.venv\Scripts\python.exe src/scripts/tools/build_parser_catalog.py --check
```

