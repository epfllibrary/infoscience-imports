# Guide d'utilisation — Infoscience Import Pipeline

## Critères d'acceptation

Une publication passe par deux niveaux de filtrage successifs avant d'être importée dans Infoscience : le niveau **item** (la publication elle-même) et le niveau **auteur** (au moins un auteur EPFL valide doit exister).

---

### Niveau item

#### 1. Collecte et détection d'affiliation EPFL

Chaque source utilise ses propres identifiants institutionnels pour détecter une affiliation EPFL parmi les auteurs de la publication :

| Source | Identifiants / patterns reconnus |
|---|---|
| Scopus | AFIDs EPFL (liste `scopus_epfl_afids`) |
| WoS | « EPFL », « Ecole Polytechnique Federale de Lausanne » (fuzzy ≥ 80 %) |
| OpenAlex / Crossref | ROR `02s376052`, « EPFL », « Polytechnique Fédérale de Lausanne », ROR `02hdt9m26` (Swiss Data Science Center) |
| Zenodo | « EPFL », « Polytechnique Fédérale de Lausanne », « Swiss Federal Institute of Technology in Lausanne » |
| DataCite | idem Zenodo (normalisé ASCII) |
| EPO | tous les inventeurs sont candidats (pas de filtre d'affiliation) |

Une publication sans aucun auteur portant une affiliation EPFL reconnue ne passe pas l'étape de collecte.

#### 2. Dédoublonnage cross-sources

Parmi les publications collectées depuis plusieurs sources lors du même run :

- **DOI identique** → doublons fusionnés, une seule entrée conservée (les métadonnées complémentaires sont mergées).
- **Titre + année identiques (sans DOI)** → dédoublonnage type-aware :
  - Dataset ≠ non-dataset → conservés séparément (entités différentes).
  - Preprint + version publiée → la version publiée remplace le preprint.
  - Deux records du même type → le record de la source prioritaire est conservé.

#### 3. Dédoublonnage Infoscience

Chaque publication est comparée à l'existant dans Infoscience (DSpace) via l'API REST :

- Correspondance trouvée par DOI ou titre+année → item marqué **dédoublonné** (non importé, visible dans l'onglet _Dédoublonnés_).
- Aucune correspondance → item retenu pour enrichissement.

#### 4. Enrichissement OA / PDF

Via Unpaywall (et OpenAlex en source secondaire) :

- `upw_is_oa` : booléen indiquant si la publication est en accès ouvert.
- `upw_license` : licence OA (ex. `cc-by`, `public-domain`).
- `upw_valid_pdf` : chemin vers un PDF téléchargé, ou `None`.

Un PDF n'est récupéré que si la licence est ouverte (famille `cc-*` ou `public-domain`). Les licences `elsevier-specific`, `publisher-specific-oa` et `implied-oa` sont considérées **non libres** et ne déclenchent pas de téléchargement.

#### 5. Critère d'acceptation final pour l'import

Un item est importé dans DSpace si et seulement si **au moins un auteur EPFL possède une unité principale valide** (`final_mainunit` non vide) après réconciliation. Sans cela, l'item est rejeté (visible dans _Rejetés_).

---

### Niveau auteur EPFL

#### 1. Détection d'affiliation EPFL (source)

L'auteur est marqué `epfl_affiliation=True` si son champ `organizations` contient un identifiant EPFL reconnu (voir tableau ci-dessus). Cette valeur est la vérité source ; elle n'est jamais modifiée par la réconciliation.

#### 2. Réconciliation DSpace + API EPFL

Pour chaque auteur avec `epfl_affiliation=True`, l'enrichisseur tente de retrouver un SCIPER en deux étapes :

1. **Lookup DSpace** : recherche d'un profil par ORCID, puis par identifiant source (Scopus ID, WoS ResearcherID, OpenAlex ID), puis par nom normalisé.
2. **Lookup API EPFL** : si un SCIPER est trouvé, récupération des accréditations actuelles pour déterminer l'unité principale et le statut.

Un auteur sans SCIPER après ces deux étapes est **non réconcilié** : il apparaît dans la modal avec l'icône `person_search` (gris) et le badge _Non réconcilié_. Il n'est pas lié dans l'item DSpace mais est traçable dans l'onglet _Auteurs EPFL détectés_.

#### 3. Vérification du statut de membre actif vs. ancien membre

Si l'API EPFL ne renvoie **aucune accréditation** pour le SCIPER trouvé, l'auteur a quitté l'EPFL. Le pipeline applique alors une des deux règles suivantes :

| Situation | Résultat |
|---|---|
| Profil DSpace présent (`dspace_uuid` renseigné) | `_check_former_member_validity` est appelé : la date de fin d'affiliation (`oairecerif.affiliation.endDate`) est comparée à l'année de publication. Si `pub_year − end_year ≤ 1` → **toléré** (`epfl_is_former=True`, `dspace_link_valid=True`). Sinon → **rejeté**. |
| Pas de profil DSpace | Rejet conservatif direct, sans vérification de date (`epfl_is_former=True`, `dspace_link_valid=False`). |

En cas d'erreur lors de la vérification de la date, l'auteur est traité comme membre actif (fallback permissif).

#### 4. Statut « faible »

Un auteur réconcilié (SCIPER trouvé, membre actif) est marqué **statut faible** si son accréditation correspond à un rôle périphérique :

- `epfl_status` vide, `Hôte`, `Hors EPFL` ou `Étudiant`
- `epfl_status = Personnel` avec une position parmi : _Academic guest, Consultant, Doctoral assistant, Engineer, External employee, External student, Guest, Guest PhD student, Lecturer, Postdoctoral researcher, Visiting professor_

Le statut faible est purement indicatif ; l'auteur est lié normalement dans DSpace (`dspace_link_valid=True`).

#### 5. Liaison dans l'item DSpace

Seuls les auteurs avec `dspace_link_valid=True` reçoivent un lien SCIPER et une affiliation EPFL dans l'item importé. Le critère est :

```
dspace_link_valid = True
  ↔ SCIPER trouvé
  ET NOT (ancien membre rejeté)
```

| État auteur | `epfl_is_former` | `dspace_link_valid` | Lié dans DSpace |
|---|---|---|---|
| Membre actif | `False` | `True` | ✅ |
| Membre actif, statut faible | `False` | `True` | ✅ (avec avertissement UI) |
| Ancien membre toléré (≤ 1 an) | `True` | `True` | ✅ |
| Ancien membre rejeté (> 1 an ou sans DSpace) | `True` | `False` | ❌ |
| Non réconcilié (pas de SCIPER) | `False` | `False` | ❌ |

---

### Résumé du flux de décision

```
Publication collectée
│
├─ Affiliation EPFL détectée ?  ──Non──► Ignorée
│  └─ Oui
│
├─ Déjà dans Infoscience ?  ──Oui──► Dédoublonné
│  └─ Non
│
├─ Au moins 1 auteur avec final_mainunit ?  ──Non──► Rejeté
│  └─ Oui
│
└─► Importé dans DSpace
     │
     ├─ Auteur + SCIPER + actif          → lié (dspace_link_valid=True)
     ├─ Auteur + SCIPER + toléré         → lié (dspace_link_valid=True)
     ├─ Auteur + SCIPER + rejeté         → non lié (dspace_link_valid=False)
     └─ Auteur sans SCIPER               → non lié, tracé dans pub_detected_authors
```
