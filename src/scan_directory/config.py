from cartellino_parser.drive_service.names import normalize_term


EXCLUDE_TERMS = [
    "cedolino",
    "cedolini",
    "busta",
    "buste",
    "paga",
    "busta paga",
    "buste paga",
]
exclude_terms_normalized = [normalize_term(term) for term in EXCLUDE_TERMS]

