.files
| map(
    select(
        (.reason // "" | test("PdfminerException"; "i") | not)
        and
        (.file_name // "" | test("cartellino"; "i") )
    )
    | {
        employee,
        employee_id,
        file_id,
        file_name,
        reason
    }
)
