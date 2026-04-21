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

- RÈGLE CRITIQUE "date_fin" (case "Au" du bloc Planification) : la date de fin n'existe que si la case à cocher à GAUCHE du champ "Au" est COCHÉE (✓). Si la case est DÉCOCHÉE (vide), le champ date à droite est grisé/inactif, même s'il affiche une valeur — c'est un placeholder Dispatch, PAS une vraie date de fin. Règles :
  • Case "Au" cochée ✓ → extraire la date du champ "Au" dans date_fin (format "DD/MM/YYYY")
  • Case "Au" décochée □ → date_fin = null (récurrence sans fin)
  Vérifie TOUJOURS l'état de la case avant d'extraire date_fin. Un champ grisé/désactivé = décoché.

- ONGLET "Informations" : un onglet dédié (séparé de Général/Ordre/Attribution/Tarification) contient :
  • "Saisi le" : date de saisie de l'ordre dans Dispatch (pour info, pas toujours utile)
  • "Commentaires" avec 3 champs : "Infos Enl." (infos enlèvement), "Infos Liv." (infos livraison), "Info Fact." (info facturation)
  TOUT contenu non vide de ces commentaires doit aller dans additional_info, préfixé par le nom du champ. Exemple : "Infos Enl.: sonner interphone 3B | Info Fact.: bon de commande BC-4521". Si tout est vide, ignore ce tab.

- Pour les sous-prestations (sp1..sp4) : ordre d'apparition dans le tableau de l'onglet Tarification. Si moins de 4 sous-prestations, laisse les slots restants null.

- CHAMP "additional_info" (POUBELLE DE LUXE — TRÈS UTILISÉ) : toute information potentiellement utile qui ne rentre dans AUCUN autre champ JSON dédié doit atterrir ici. Exemples à capturer systématiquement :
  • Commentaires libres, notes manuscrites, champs "Instructions" / "Commentaire" / "Informations" remplis
  • Numéros/références inhabituels (code porte, bon de commande, dossier client, n° BL, n° tour)
  • Consignes spécifiques (sonner à l'interphone, prévenir 30 min avant, livraison contre signature, refus partiel autorisé, etc.)
  • Conditionnement particulier, sensibilité de la marchandise, contrainte d'accès quai/nacelle
  • Plages horaires alternatives ou exceptions ("sauf lundi", "pas le 1er du mois", "férié = livré la veille")
  • Tout ce que tu vois écrit à l'écran et qui n'a pas de colonne dédiée
  Format : texte libre français, jusqu'à ~500 caractères. Plusieurs infos = sépare par " | ". Si vraiment rien de notable → null. Ne duplique JAMAIS ici une donnée déjà présente dans un autre champ.

- BON SENS sur la planification (IMPORTANT) : le bloc Planification peut contenir PLUSIEURS sous-blocs (Hebdomadaire/Mensuelle). Applique ton bon sens :
  • Si les sous-blocs sont STRICTEMENT identiques (mêmes dates, même périodicité, mêmes jours) → c'est une duplication buggée de Dispatch : n'en garde qu'UN pour remplir les champs, et note-le dans claude_comment.
  • Si les sous-blocs se COMBINENT (ex: bloc A = lundi, bloc B = mercredi) → fusionne les jours dans jours_semaine (ex: "lun,mer").
  • Si les sous-blocs ont des dates de début différentes → prends la PLUS ANCIENNE pour date_debut et la plus tardive pour date_fin, et note-le dans claude_comment.
  • Si les sous-blocs ont des périodicités MIXTES (Hebdo + Mensuelle) → remplis selon la première et note le conflit dans claude_comment (ce cas demandera une vérif humaine).
  • Vérifie TOUJOURS le calendrier Gantt pour confirmer quels jours sont réellement actifs — c'est la source de vérité en cas de conflit avec le texte.

- CHAMP "claude_comment" (JOURNAL D'EXTRACTION STRUCTURÉ — OBLIGATOIRE) : rapport sur la qualité de ton extraction. Format STRICT avec tags entre crochets, séparés par " | " :
  [ILLISIBLE] liste des champs dont tu vois qu'un cadre/label existe mais dont la valeur est illisible (texte flou, coupé, bug de rendu Dispatch type "pas redessiné", zone grisée) → NE DEVINE PAS, mets null dans le JSON et liste ici.
  [DEVINÉ] champs pour lesquels tu as fait une best guess pas 100% sûre (texte partiellement visible, Gantt ambigu, reconstruction des lettres espacées de Dispatch).
  [ATTENTION] incohérences détectées (Gantt ≠ texte, date_fin vide pour récurrence active, Mensuelle sans days_of_month visibles, plusieurs blocs Planification, montants absurdes, contact vide, etc.).
  [OK] phrase courte sur ce qui est clair et fiable.
  Exemples :
    "[ILLISIBLE] enl_contact_tel, liv_horaire | [DEVINÉ] jours_semaine (Gantt peu lisible mais texte cohérent) | [OK] reste"
    "[ATTENTION] 2 blocs Planification Hebdo identiques → duplication Dispatch, un seul gardé | [OK] tout le reste"
    "[OK] extraction nette, aucun doute"
  RÈGLES :
  - Ne renvoie JAMAIS null si tu as extrait au moins une valeur
  - Les tags [ILLISIBLE] et [ATTENTION] sont des signaux de relance → sois exhaustif, ne rate rien
  - Si un champ visuel est totalement vide dans l'UI (pas de cadre/label), n'en parle pas (c'est normal)
  - Ce champ n'est JAMAIS dupliqué dans additional_info

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
        "- additional_info : info importante vue dans les screenshots qui ne rentre dans aucun autre champ (null si rien). Inclut TOUT commentaire non vide de l'onglet Informations (Infos Enl. / Infos Liv. / Info Fact.) préfixé par le nom du champ.\n"
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
