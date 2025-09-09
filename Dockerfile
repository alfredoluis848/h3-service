# Etapa de build base: Python slim (leve)
FROM python:3.11-slim AS base

# Não rodar como root
RUN useradd -m h3user

# Diretório de trabalho
WORKDIR /app

# Copiar dependências primeiro (para aproveitar cache)
COPY requirements.txt .

# Instalar dependências
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação
COPY app.py .
COPY tests ./tests

# Ajustar permissões
RUN chown -R h3user:h3user /app

# Alternar para usuário não-root
USER h3user

# Expor porta interna
EXPOSE 8000

# Comando de inicialização
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
