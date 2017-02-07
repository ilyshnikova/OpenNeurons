from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Manager:
    def __init__(self, db_name, user='postgres', host='localhost'):
        self.user = user
        self.host = host
        self.db_name = db_name
        self.engine = create_engine(
            'postgresql://{user}@{host}:5432/{db_name}'.format(user=user, host=host, db_name=db_name),
            echo=True
        )
        self.session = sessionmaker(bind=self.engine)()

    def close(self):
        self.session.close()

    def rollback(self):
        self.session.rollback()

