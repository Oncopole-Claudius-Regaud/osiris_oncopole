import os


def load_sql(filename: str) -> str:
    """
    Charge le contenu d'un fichier SQL situé dans le dossier sql/
    à partir du chemin absolu fiable, peu importe le cwd de l'exécution.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(base_dir, "..", "sql", filename)
    with open(sql_path, 'r', encoding='utf-8') as f:
        return f.read()
