"""Order data schema — single source of truth for CSV columns and Vision JSON."""

from __future__ import annotations

CSV_COLUMNS: list[str] = [
    "code_ordre", "libelle_ordre",
    "code_client", "nom_client",
    "donneur_ordre", "reference_1", "reference_2", "reference_3", "nb_pp",
    "periodicite", "date_debut", "date_fin", "frequence_intervalle",
    "jours_semaine", "jours_semaine_uncertain", "jours_feries",
    "enl_nom", "enl_no", "enl_rue", "enl_complement", "enl_cp", "enl_ville", "enl_pays",
    "enl_horaire_type", "enl_horaire", "enl_horaire_2",
    "enl_contact_nom", "enl_contact_tel", "enl_contact_email",
    "liv_nom", "liv_no", "liv_rue", "liv_complement", "liv_cp", "liv_ville", "liv_pays",
    "liv_horaire_type", "liv_horaire", "liv_horaire_2",
    "liv_contact_nom", "liv_contact_tel", "liv_contact_email",
    "distance", "duree",
    "code_prestation", "libelle_prestation",
    "sp1_code", "sp1_libelle", "sp1_qte", "sp1_prix_u", "sp1_montant",
    "sp2_code", "sp2_libelle", "sp2_qte", "sp2_prix_u", "sp2_montant",
    "sp3_code", "sp3_libelle", "sp3_qte", "sp3_prix_u", "sp3_montant",
    "sp4_code", "sp4_libelle", "sp4_qte", "sp4_prix_u", "sp4_montant",
    "conducteur_code", "conducteur_nom",
    "vehicule_code", "vehicule_libelle", "remorque",
    "montant_total",
    "status", "champs_manquants", "screenshots_dir", "extracted_at",
]

# JSON schema sent to Claude Vision — same keys as CSV_COLUMNS minus the run metadata.
VISION_JSON_KEYS: list[str] = [c for c in CSV_COLUMNS if c not in {
    "status", "champs_manquants", "screenshots_dir", "extracted_at",
}]


def empty_row() -> dict[str, str]:
    """Return a dict with all CSV columns set to empty string."""
    return {c: "" for c in CSV_COLUMNS}


def normalize_row(data: dict) -> dict[str, str]:
    """Take a (partial) dict and return a full row with all columns, empty for missing ones."""
    row = empty_row()
    for k, v in (data or {}).items():
        if k in row:
            row[k] = "" if v is None else str(v)
    return row
