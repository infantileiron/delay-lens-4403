from sqlalchemy import create_engine,text
from sqlalchemy.orm import DeclarativeBase,sessionmaker
from .settings import DATABASE_URL
class Base(DeclarativeBase): pass
engine=create_engine(DATABASE_URL,pool_pre_ping=True)
Session=sessionmaker(bind=engine,expire_on_commit=False)
def get_db():
 with Session() as db: yield db
# Columns added for crash-resume/fencing; create_all covers fresh databases,
# this backfills existing deployments without destructive migrations.
RUN_RESUME_COLUMNS={"attempts":"INT NOT NULL DEFAULT 0","fence":"INT NOT NULL DEFAULT 0"}
def initialize():
 from .. import models
 Base.metadata.create_all(engine)
 if engine.dialect.name=="mysql":_ensure_run_columns()
def _ensure_run_columns():
 with engine.begin() as conn:
  existing={r[0] for r in conn.execute(text("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='runs'"))}
  for name,ddl in RUN_RESUME_COLUMNS.items():
   if name not in existing:conn.execute(text(f"ALTER TABLE runs ADD COLUMN {name} {ddl}"))
