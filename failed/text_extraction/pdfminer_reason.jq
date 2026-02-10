[
  .files
  | to_entries[]
  | select(
      (.value.reason // "")
      | contains("PdfminerException")
    )
  | {
      file_id: .key,
      employee: .value.employee,
      file_name: .value.file_name,
      reason: .value.reason
    }
]
