# dispatch-script

Extracteur d'ordres réguliers depuis **Dispatch INNOVIA** (VM Windows) vers CSV, via pywinauto + Claude Vision.

## Installation (VM Windows)

```powershell
winget install Python.Python.3.11
git clone https://github.com/AlexandreGRN/dispatch-script.git
cd dispatch-script
pip install -r requirements.txt
setx ANTHROPIC_API_KEY "sk-ant-xxxxxxxx"
```

**Redémarre PowerShell** après `setx` pour que la variable soit visible.

## Workflow complet

### 1. Diagnostic (une seule fois, pour valider l'approche)

1. Ouvre **Dispatch INNOVIA** → fenêtre "Liste des ordres réguliers"
2. Clique sur la **1ère ligne** (surbrillance bleue)
3. Lance :
   ```powershell
   python 00_diagnostic.py
   ```
4. Le script ouvre un ordre, clique sur chaque onglet, screenshot tout, dump l'arbre UIA
5. **Commit + push** le dossier `diagnostic/` créé → Alexandre partage l'URL pour analyse

### 2. Calibration (une seule fois, ~1 min)

1. Ouvre **manuellement** un ordre (F10) pour avoir la fenêtre détail visible avec tous les onglets
2. Lance :
   ```powershell
   python 01_calibrate.py
   ```
3. Suis les prompts : tu survoles chaque onglet/bouton avec la souris, puis Entrée
4. Génère `config.json` avec les coordonnées

### 3. Test dry-run (3 ordres, ~1 min, ~0.12 $)

1. Retourne à la **liste** (ferme l'ordre), clique 1ère ligne
2. Lance :
   ```powershell
   python 02_extract.py --dry-run 3
   ```
3. **Ne touche pas la souris** pendant ~1 min (failsafe: souris dans le coin haut-gauche → abort)
4. Vérifie `output/orders.csv` : les 3 lignes doivent avoir des champs cohérents

### 4. Full run (~300 ordres, ~45 min, ~12 $)

1. Reviens à la **liste**, clique la 1ère ligne
2. Lance :
   ```powershell
   python 02_extract.py
   ```
3. Laisse tourner sans toucher. Le script s'arrête automatiquement après `VIA33MON`
4. Résultat : `output/orders.csv` + `output/screenshots/<code>/*.png` par ordre

### Reprise après crash

Relance juste `python 02_extract.py` — les codes déjà dans `output/processed.csv` sont skippés.

## Navigation Dispatch (référence)

| Action | Méthode |
|---|---|
| Ligne suivante dans liste | `↓` (auto-scroll) |
| Ouvrir détail ordre | `F10` |
| Changer d'onglet | Clic souris (coords fixes de `config.json`) |
| Fermer détail | `Échap` + `Enter` (valide "Oui" à la popup) |

## Options `02_extract.py`

```
--dry-run N        : traite N ordres puis s'arrête
--stop-code CODE   : s'arrête après ce code (défaut: VIA33MON)
--no-vision        : ne fait que les screenshots (pas d'appel API)
```

## Fichiers produits

```
output/
├── orders.csv           # résultat final (append incrémental)
├── processed.csv        # checkpoint reprise (code, status, timestamp)
├── errors.log           # erreurs détaillées
└── screenshots/
    └── <code>/          # 7 PNG + vision_raw.json par ordre
```

## Structure du repo

- `00_diagnostic.py` — inspection d'1 ordre
- `01_calibrate.py` — capture coords interactive
- `02_extract.py` — boucle extraction full
- `lib/navigation.py` — helpers clics/touches/wait
- `lib/vision.py` — wrapper Claude API
- `lib/schema.py` — colonnes CSV (source de vérité)
- `lib/checkpoint.py` — load/save checkpoint + CSV
