[
  .[]
  | select(
      (.source_txt // "")
      | test("cartellino"; "i")
    )
]
