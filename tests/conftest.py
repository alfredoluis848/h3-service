import sys, os
# Adiciona a pasta raiz do projeto (um nível acima de tests/) ao PYTHONPATH
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
