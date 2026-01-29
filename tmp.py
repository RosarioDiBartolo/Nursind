from drive_scanner.index_service import Index

 

included = Index.load_index("new_extr/included.index.json")
includedFileMap = { file.file_id: file for file in included.files }
 
keys = includedFileMap.keys()
full = Index.load_index("scan/full.index.json")

diff = [ f for f in full.files if f.file_id not in keys  ]


excluded = Index.load_index("new_extr/excluded.index.json") 
excludedFileMap = { file.file_id: file for file in excluded.files }

 
excluded.files = [ excludedFileMap.get(file.file_id, file)  for file in diff]

excluded.save_index("./excluded.index.json")