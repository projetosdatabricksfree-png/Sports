"""
Apache Superset — Configuração de produção mínima
"""
import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "CHANGE_IN_PRODUCTION")

# Banco de metadados
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://admin:admin@postgres:5432/superset_metadata"
)

# Cache (Redis opcional — desativado no modo local)
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache"}

# Feature flags recomendadas
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_RBAC": True,
}

# Segurança
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False   # True em produção com HTTPS
SESSION_COOKIE_SAMESITE = "Lax"

# Uploads
UPLOAD_FOLDER = "/app/superset_home/uploads"
ROW_LIMIT = 5000
