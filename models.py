from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    login = Column(String, nullable=False)

class Mode(Base):
    __tablename__ = 'modes'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    name = Column(String, nullable=False)
    host = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    alias = Column(String, nullable=False)
    is_active = Column(Integer, default=0)

class UserPort(Base):
    __tablename__ = 'user_ports'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    port = Column(Integer, unique=True, nullable=False)
    proxy_node = Column(String, nullable=True)
    algo = Column(String, nullable=True)

def init_db(db_url=None):
    if db_url is None:
        from .config import DATABASE_URL
        db_url = DATABASE_URL
    eng = create_engine(db_url)
    return eng

def get_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()
