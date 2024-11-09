@echo off
REM Script pour Windows : Créer et configurer un environnement virtuel, installer les packages et ajouter le kernel Jupyter

echo Création de l'environnement virtuel...
python -m venv env

echo Activation de l'environnement virtuel...
call env\Scripts\activate

echo Mise à jour de pip...
pip install --upgrade pip

echo Installation des packages requis à partir de requirements.txt...
pip install -r requirements.txt

echo Installation de Jupyter et ipykernel...
pip install jupyter ipykernel

echo Ajout du kernel de l'environnement à Jupyter...
python -m ipykernel install --user --name env --display-name "Python (env)"

echo Environnement prêt. Lancer Jupyter Notebook dans cet environnement avec la commande : jupyter notebook
pause
