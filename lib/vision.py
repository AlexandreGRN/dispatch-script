"""Claude Vision wrapper: 8 screenshots of one order → structured JSON dict."""

from __future__ import annotations

import base64
import json
import os
import time
from pathlib import Path

from anthropic import Anthropic

from .schema import VISION_JSON_KEYS

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 4000

SYSTEM_PROMPT = """Tu es un extracteur de données structurées. Tu reçois plusieurs screenshots d'un ordre régulier dans le logiciel TMS "Dispatch INNOVIA", pris sur différents onglets (Général, Ordre avec sous-onglets Enlèvement/Livraison/Contact, Attribution, Tarification).

Ta mission : extraire STRICTEMENT les champs demandés depuis ce que tu VOIS dans les images. Règles absolues :
- Si un champ n'est pas visible ou vide dans les screenshots : renvoie null.
- N'invente JAMAIS une valeur. Préfère null à une supposition.
- Les dates : format "DD/MM/YYYY" tel qu'affiché.
- Les heures : format "HH:MM" tel qu'affiché.
- Les montants : nombre décimal avec point (ex: 395.00).
- Les jours de la semaine : liste séparée par des virgules avec abréviations courtes (lun,mar,mer,jeu,ven,sam,dim).
  ATTENTION RENDU DISPATCH : le logiciel affiche parfois les jours avec des espaces parasites entre chaque lettre (bug graphique). Exemple : "ma r d i"="mardi", "me r c r ed i"="mercredi", "l u n d i"="lundi", "j eu d i"="jeudi", "v en d r ed i"="vendredi". Reconstitue le mot complet lettre par lettre.
  VALIDATION OBLIGATOIRE via le calendrier Gantt : l'onglet Général contient un calendrier visuel (cases vertes/bleues par semaine). Utilise-le pour CONFIRMER les jours extraits du texte. Si le Gantt montre des coches sur mardi-vendredi mais le texte dit "lundi-vendredi", signale la discordance avec le champ supplémentaire "jours_semaine_uncertain": true. Si concordance → "jours_semaine_uncertain": false.

- RÈGLE SPÉCIFIQUE PERIODICITE="Mensuelle" : le champ "jours_semaine" n'est PAS pertinent → renvoie-le à null ET "jours_semaine_uncertain": false. À la place, remplis "days_of_month" = liste d'entiers entre 1 et 31 (JSON array: [1], [1, 15], [5, 20], etc.) correspondant aux jours du mois actifs. Le Gantt Mensuel est organisé en lignes = mois et colonnes = jours 1→31 : les cases avec coche verte ✓ indiquent les jours actifs. Si un seul jour (ex: le 1er de chaque mois), renvoie [1]. Pour les periodicites non-Mensuelles, renvoie "days_of_month": null.

- Pour les sous-prestations (sp1..sp4) : ordre d'apparition dans le tableau de l'onglet Tarification. Si moins de 4 sous-prestations, laisse les slots restants null.

- CHAMP "additional_info" : si tu vois dans les screenshots des informations potentiellement importantes qui ne rentrent dans aucun autre champ (commentaire spécifique, note manuscrite, champ libre rempli, particularité visuelle, numéro ou référence inhabituelle, mention d'un conditionnement spécial, etc.), résume-les brièvement en français dans ce champ (max 2-3 phrases). Si rien de notable, renvoie null. Ne duplique JAMAIS ici une donnée déjà présente dans un autre champ.

- CHAMP "claude_comment" (TRÈS IMPORTANT — AUTO-ÉVALUATION) : utilise ce champ pour DIRE comment s'est passée l'extraction de ton point de vue. Liste :
  • ce qui était clair et extrait avec certitude (concision),
  • ce qui était ambigu / partiellement visible / nécessitait une devinette (DÉTAILLE, c'est crucial),
  • les champs que tu as mis à null alors qu'ils existaient probablement mais illisibles,
  • les incohérences détectées (ex: Gantt ≠ texte, date_fin vide pour une récurrence active, periodicite Mensuelle mais pas de days_of_month visibles, montants non cohérents, etc.),
  • tout ce qui mérite une vérification humaine.
  Écris en français, format libre, max 5 phrases courtes ou 5 bullet points. NE RENVOIE JAMAIS null pour ce champ si tu as extrait au moins une valeur — il doit contenir au minimum une phrase. Si tout est propre et sans doute : "Extraction nette, tous les champs visibles sont extraits avec certitude." Ce champ n'est JAMAIS dupliqué dans un autre.

- Réponds UNIQUEMENT avec un JSON valide, sans markdown, sans commentaire, sans texte avant/après."""


def _build_schema_prompt() -> str:
    keys_bullet = "\n".join(f"- {k}" for k in VISION_JSON_KEYS)
    return (
        "Renvoie un JSON avec EXACTEMENT ces clés (toutes présentes, valeur null si inconnue) :\n"
        f"{keys_bullet}\n\n"
        "Contexte des champs importants :\n"
        "- code_ordre, libelle_ordre, code_client, nom_client : onglet Général / titre fenêtre.\n"
        "- donneur_ordre, reference_1/2/3 : onglet Ordre, en haut.\n"
        "- nb_pp : onglet Général (bloc planification ou section principale).\n"
        "- periodicite : 'Hebdomadaire' / 'Mensuelle' / 'Annuelle' / 'Quotidienne'.\n"
        "- date_debut, date_fin : bloc Planification, champs 'Du' et 'Au'.\n"
        "- frequence_intervalle : nombre dans 'Toutes les X semaines/mois'.\n"
        "- jours_semaine : cases cochées dans 'Le lun/mar/...'.\n"
        "- jours_feries : 'Jours fériés exclus' ou 'Jours fériés inclus'.\n"
        "- enl_*, liv_* : onglet Ordre, blocs Enlèvement (gauche) et Livraison (droite). "
        "enl_horaire_type = 'à' / 'avant' / 'après'. enl_horaire_2 = 2ème heure si présente.\n"
        "- enl_contact_*, liv_contact_* : sous-onglet Contact de chaque bloc.\n"
        "- distance, duree : onglet Ordre, en haut à droite.\n"
        "- code_prestation, libelle_prestation : onglet Tarification, ligne 'Prestation'.\n"
        "- sp1..sp4 : onglet Tarification, tableau sous-prestations.\n"
        "- conducteur_*, vehicule_*, remorque : onglet Attribution.\n"
        "- montant_total : onglet Tarification, 'Montant' ou 'Vente'.\n"
        "- days_of_month : UNIQUEMENT pour periodicite='Mensuelle', liste d'entiers 1-31 extraits du Gantt mensuel (null sinon).\n"
        "- additional_info : info importante vue dans les screenshots qui ne rentre dans aucun autre champ (null si rien).\n"
        "- claude_comment : auto-évaluation de l'extraction (ce qui a été clair, ce qui a été ambigu, incohérences détectées, champs illisibles, ce qui mérite vérification humaine). JAMAIS null si tu as extrait au moins une valeur — au minimum une phrase. Cf. système.\n"
    )


def _encode_image(path: Path) -> dict:
    data = path.read_bytes()
    return {
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": "image/png",
            "data": base64.b64encode(data).decode("ascii"),
        },
    }


def _parse_json_strict(text: str) -> dict | None:
    """Try to parse JSON, stripping optional code fences."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        # drop optional "json" language hint on first line
        if "\n" in cleaned:
            first, rest = cleaned.split("\n", 1)
            if first.strip().lower() in ("json", ""):
                cleaned = rest
    # find the outermost {...}
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1:
        return None
    try:
        return json.loads(cleaned[start : end + 1])
    except json.JSONDecodeError:
        return None


class VisionExtractor:
    def __init__(self, api_key: str | None = None):
        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("ANTHROPIC_API_KEY is not set")
        self.client = Anthropic(api_key=key)

    def extract(self, screenshots: dict[str, Path]) -> tuple[dict | None, str]:
        """Send all screenshots in one request, return (data_dict, raw_response_text)."""
        content: list[dict] = []
        for label, path in screenshots.items():
            content.append({"type": "text", "text": f"Screenshot: {label}"})
            content.append(_encode_image(path))
        content.append({"type": "text", "text": _build_schema_prompt()})

        for attempt in range(2):
            resp = self.client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                temperature=0,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": content}],
            )
            text = "".join(
                block.text for block in resp.content if getattr(block, "type", "") == "text"
            )
            data = _parse_json_strict(text)
            if data is not None:
                return data, text
            # Retry once with an explicit nudge.
            content = [
                *content,
                {
                    "type": "text",
                    "text": (
                        "Ta dernière réponse n'était pas un JSON valide. "
                        "Réponds UNIQUEMENT avec le JSON demandé, rien d'autre."
                    ),
                },
            ]
            time.sleep(0.5)

        return None, text
