from sqlalchemy import text, create_engine

class ExecuteQuery:
    @staticmethod
    def execute(config):
        print("Consuming: " + config.host)
        engine = create_engine(
            f'{config.engine}://{config.user}:{config.password}@{config.host}:{config.port}/{config.db}')
        with engine.connect() as connection:
            result = connection.execute(text(config.query))
            return result.mappings().all()
