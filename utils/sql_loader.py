# Charge le contenu brut d’un fichier SQL et le retourne sous forme de chaîne de caractères
def load_sql(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

