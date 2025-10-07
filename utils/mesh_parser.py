from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import date


def _xml_date(node):
    if node is None:
        return None
    try:
        return date(int(node.findtext('Year')),
                    int(node.findtext('Month')),
                    int(node.findtext('Day')))
    except Exception:
        return None


def parse_mesh(xml_path: Path, out_dir: Path = Path("/tmp/mesh")) -> None:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    desc, qual, conc = [], [], []
    trees, actions = [], []

    for dr in root.findall('DescriptorRecord'):
        mesh_id = dr.findtext('DescriptorUI')

        # previous indexing (liste)
        prev = [e.text.strip() for e in dr.findall("PreviousIndexingList/PreviousIndexing") if e.text]

        desc.append({
            "mesh_id": mesh_id,
            "label_fr": dr.findtext('DescriptorName/StringFR'),
            "label_en": dr.findtext('DescriptorName/StringUS'),
            "date_created": _xml_date(dr.find('DateCreated')),
            "date_revised": _xml_date(dr.find('DateRevised')),
            "date_established": _xml_date(dr.find('DateEstablished')),
            "history_note": (dr.findtext('HistoryNote') or '').strip(),
            "public_note": (dr.findtext('PublicMeSHNote') or '').strip(),
            "scope_note": (dr.findtext(".//Concept[@PreferredConceptYN='Y']/ScopeNote") or '').strip(),
            "online_note": (dr.findtext("OnlineNote") or '').strip(),
            "previous_indexing": prev,
        })

        for aq in dr.findall('AllowableQualifiersList/AllowableQualifier'):
            qual.append({
                "mesh_id": mesh_id,
                "qualifier_ui": aq.findtext('QualifierReferredTo/QualifierUI'),
                "label_fr": aq.findtext('QualifierReferredTo/QualifierName/StringFR'),
                "label_en": aq.findtext('QualifierReferredTo/QualifierName/StringUS'),
                "abbr": aq.findtext('Abbreviation')
            })

        for c in dr.findall('ConceptList/Concept'):
            conc.append({
                "mesh_id": mesh_id,
                "concept_ui": c.findtext('ConceptUI'),
                "preferred_yn": c.get('PreferredConceptYN') == "Y",
                "label": c.findtext('ConceptName/String'),
                "registry_number": c.findtext('RegistryNumber'),
                "cas_name": c.findtext('CASN1Name'),
                "scope_note": (c.findtext('ScopeNote') or '').strip(),
                "related_registry_numbers": [e.text for e in c.findall("RelatedRegistryNumberList/RelatedRegistryNumber") if e.text]
            })
        for tn in dr.findall('TreeNumberList/TreeNumber'):
            if tn.text:
                trees.append({"mesh_id": mesh_id, "tree_number": tn.text.strip()})

        for pa in dr.findall('PharmacologicalActionList/PharmacologicalAction'):
            actions.append({
                "mesh_id": mesh_id,
                "action_ui": pa.findtext("DescriptorReferredTo/DescriptorUI"),
                "action_name": pa.findtext("DescriptorReferredTo/DescriptorName/String")
            })

    out_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(desc).to_csv(out_dir / "descriptors.csv", index=False)
    pd.DataFrame(qual).to_csv(out_dir / "qualifiers.csv", index=False)
    pd.DataFrame(conc).to_csv(out_dir / "concepts.csv", index=False)
    pd.DataFrame(trees).to_csv(out_dir / "tree_numbers.csv", index=False)
    pd.DataFrame(actions).to_csv(out_dir / "pharma_actions.csv", index=False)
