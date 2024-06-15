from sqlalchemy import text, create_engine
import logging

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)-10s) %(message)s',)

class ExecuteQuery:
    @staticmethod
    def execute(config):
        print("Consuming: " + config.host)
        engine = create_engine(
            f'{config.engine}://{config.user}:{config.password}@{config.host}:{config.port}/{config.db}')
        with engine.connect() as connection:
            try:
                trans = connection.begin()
                result = connection.execute(text(config.query))
                trans.commit()
                return result.mappings().all()
            except Exception as e:
                    logging.error(f"An error occurred: {e}")
                    trans.rollback()
