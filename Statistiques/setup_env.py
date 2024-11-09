import os
import subprocess
import sys

# Nom de l'environnement virtuel
env_name = "env"

# Créer l'environnement virtuel
print("Création de l'environnement virtuel...")
subprocess.run([sys.executable, "-m", "venv", env_name])

# Activer et installer les packages requis (pour Windows)
if os.name == 'nt':
    activation_command = f"{env_name}\\Scripts\\activate && pip install --upgrade pip && pip install -r requirements.txt"
else:
    activation_command = f"source {env_name}/bin/activate && pip install --upgrade pip && pip install -r requirements.txt"

# Exécuter la commande d'activation et d'installation
print("Activation de l'environnement virtuel et installation des packages à partir de requirements.txt...")
subprocess.run(activation_command, shell=True)

print("Environnement prêt à être utilisé. Vous pouvez maintenant l'utiliser dans VSCode.")
