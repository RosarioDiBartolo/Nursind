# Mappatura documentale Drive

La pipeline di mappatura legge tutti i PDF di una cartella Drive e delle sue
sottocartelle. Non crea, sposta o modifica file su Drive. I file non PDF restano
nell'inventario con il percorso originale. I metadati macOS vengono ignorati;
gli ZIP vengono inventariati, ma non aperti. Gli shortcut non vengono seguiti.

Usa la stessa configurazione della pipeline principale, eventualmente in un
file separato con `drive.root_id` e `pipeline.name` dedicati:

```powershell
.\.venv\Scripts\python.exe src\scripts\map_drive.py --config pipeline.json
```

Step eseguibili separatamente:

```powershell
.\.venv\Scripts\python.exe src\scripts\map_drive.py --through scan
.\.venv\Scripts\python.exe src\scripts\map_drive.py --from extract_documents --through extract_documents
.\.venv\Scripts\python.exe src\scripts\map_drive.py --from extract_events
```

## Riuso e output

- `scan`: riusa la lettura Drive e `MapIndex`; inventaria senza filtri sui nomi
  né supposizioni sul dipendente a partire dalle sottocartelle.
- `extract_documents`: esegue **lo stesso step** della pipeline principale,
  con le stesse opzioni OCR e gli stessi JSON canonici. La mappatura forza
  `limit=0` e rielabora inclusi/esclusi per aggiornare i documenti modificati.
- `extract_events`: riusa `parse_document_pages` e i parser della pipeline
  principale su ogni pagina separatamente, aggiungendo l'identità dal contenuto.
  Non esegue pulizia, abbinamento dei turni o aggregazioni.

Gli artefatti sono isolati in `<output-pipeline>/mapping/`:

```text
scan/inventory.json          Tutti i file, ID, tipo e percorso relativo
scan/included.index.json     PDF destinati all'estrazione esistente
documents/                  JSON canonici, manifest e indici di estrazione
events/events.csv           Eventi con dipendente e riferimento pagina
events/pages.csv            Esito, identità, periodo e motivi per pagina
mapping.json                Mappa completa file/pagine, candidati ed errori
```

Una pagina è riconosciuta come timbrature se il parser esistente emette almeno
un evento valido con periodo ricavato dal contenuto. Nessun periodo viene
indovinato dal nome del file. Pagine senza eventi, con periodo mancante o con
errori non sono riconosciute. Un file è riconosciuto se contiene almeno una
pagina riconosciuta e non ha errori di elaborazione. Un errore di elaborazione
di una pagina invalida il riconoscimento del file; gli errori restano nella mappa.
Un errore di estrazione non produce un riconoscimento positivo.

L'identità viene letta da etichette esplicite `Dipendente`, `Nominativo`,
`Cognome e nome` o `Cognome/nome`, anche sulla riga successiva, e coppie
`Cognome: ... Nome: ...` sulla stessa riga. Il riconoscimento
è deterministico e prudente: non è un'identificazione universale di ogni modello
PDF. I nomi non etichettati restano da verificare; nuovi formati vanno aggiunti
sulla base di esempi. I nomi delle cartelle e dei file non sono prove d'identità.
Se tutte le identità esplicite del documento concordano, le pagine senza nome
possono usare quell'identità con stato `document_unique`. Se compaiono persone
diverse, le pagine senza nome restano senza assegnazione. Più nomi espliciti nella
stessa pagina producono `ambiguous`, con i candidati conservati. Una pagina può
essere timbrature con identità ancora mancante: i due esiti sono distinti.

Un elenco Drive incompleto per errori di accesso interrompe la scansione.
Per aggiornare la cartella rieseguire tutti gli step; per provare correzioni
al riconoscimento riusare i JSON con `--from extract_events`.
L'organizzazione in una nuova cartella Drive è esclusa da questa fase.

## Logging

Il logging normale mostra inizio/fine degli step e durata, cartelle scansionate,
avanzamento per file, conteggi di pagine/eventi/identità e percorsi degli output.
Gli errori e i PDF non ancora estratti vengono segnalati come warning.
Con `--verbose` vengono mostrati anche gli esiti per pagina. Il testo estratto,
i nomi riconosciuti dal contenuto e i dettagli delle eccezioni di analisi restano
negli artefatti; i log della mappatura riportano nomi/percorso/ID dei file.
