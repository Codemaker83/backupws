import psycopg2
import psycopg2.extras
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("vx_postgres")


class BasePostgres(object):
    """ Class created to execute SQL commands by a given database
    """
    __conn = None
    __cursor = None

    def __init__(self, config=None):
        """ Connect to database through config dict, which contains configuration parameters
        """
        str_conn = ""
        for each in config.keys():
            str_conn = '%s %s=%s' % (str_conn, each, config.get(each))
        try:
            logger.debug("Stablishing connection with db server")
            self.__conn = psycopg2.connect(str_conn)
            if config.get("isolation_level", True):
                self.__conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            self.__cursor = self.__conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except Exception as e:
            logger.debug("Connection to database not established: %s", e.message)
            self.disconnect()
            raise

    def execute(self, sql_str):
        try:
            logger.debug("SQL: %s", sql_str)
            self.__cursor.execute(sql_str)
            if self.__cursor.rowcount == -1:
                return True
        except Exception as e:
            logger.debug("Impossible to execute sql: %s", e.message)
            return 0
        return 1

    def disconnect(self):
        logger.debug("Diconnecting from db server")
        if self.__cursor:
            self.__cursor.close()
        if self.__conn:
            self.__conn.close()

    def __del__(self):
        self.disconnect()

    def __exit__(self, type, value, traceback):
        self.disconnect()

    def __enter__(self):
        return self
