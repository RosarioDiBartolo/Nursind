# Configurazione di pipeline.json

Lo schema canonico è [`pipeline.schema.json`](../pipeline.schema.json).
L'esempio completo è [`pipeline.example.json`](../pipeline.example.json).
Il file personale `pipeline.json` resta escluso da Git.

## Struttura

- `$schema`: `./pipeline.schema.json`, per suggerimenti e validazione negli editor compatibili.
- `pipeline.name`: nome della pipeline e della sottocartella dei risultati. Facoltativo: se manca si usa il nome della cartella Drive.
- `pipeline.base_output_dir`: cartella base dei risultati, predefinita `output`. Supporta percorsi assoluti e relativi alla radice del progetto, anche quando il file di configurazione è altrove.
- `drive.root_id`: unico campo obbligatorio; ID non vuoto della cartella Google Drive.
- `drive.folder_name`: nome Drive disponibile localmente, usato se manca `pipeline.name`. Sono supportati anche gli alias `drive.name` e `drive.root_name`, in questo ordine.
- `steps`: opzioni facoltative dei singoli passaggi. I valori omessi usano i default descritti nello schema.

I risultati vengono scritti in `<base_output_dir>/<nome-pipeline>/`, con caratteri non adatti al filesystem sostituiti nel nome. `output/` è esclusa da Git; basi personalizzate richiedono una propria regola se si trovano nel repository.

## Opzioni dei passaggi

Lo schema elenca tipi, limiti e default di tutti i parametri letti dal runner.
`extract_documents.ocr.enabled` è `false` per default: impostarlo a `true` per usare Tesseract sui documenti che lo richiedono.
`extract_documents.limit` uguale a `0` non limita il numero di documenti.
`download_workers: null` lascia la scelta al comportamento predefinito del motore di estrazione.
`summarize_shifts.format` accetta `csv` o `json`; gli anni e `min_hours` possono essere `null` quando non si vuole applicare un filtro esplicito.

`clean_shifts_export` e `turni_custom_counts` configurano strumenti opzionali, eseguiti separatamente dai sette passaggi canonici. Aggiungerli a `steps` non li avvia automaticamente.

## Validazione

Gli oggetti rifiutano campi sconosciuti nello schema, così l'editor può segnalare errori di battitura. I default sono descrittivi: lo schema non modifica il file.
Il collegamento `$schema` abilita la validazione nell'editor; il programma conserva i propri controlli esistenti e non applica automaticamente l'intero JSON Schema durante l'avvio.
Non inserire credenziali nel file: continuano a essere lette dall'ambiente o da `.env`.
