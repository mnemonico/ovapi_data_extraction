from configuration import DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT, DATABASE_NAME
from sqlalchemy import Table, MetaData, inspect, create_engine, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from datetime import datetime
from logger import usecase_logger

logger = usecase_logger(__name__)

meta = MetaData()
db_engine = create_engine(url="postgresql://{user}:{password}@{host}:{port}/{name}".format(user=DATABASE_USER,
                                                                                           password=DATABASE_PASSWORD,
                                                                                           host=DATABASE_HOST,
                                                                                           port=DATABASE_PORT,
                                                                                           name=DATABASE_NAME))


def session_instance():
    logger.debug('initiating database session')
    return Session(db_engine)


def table_if_exists(tablename=None, engine=None):
    assert isinstance(tablename, str), 'tablename should be string'
    assert engine is not None, 'you should instantiate the database engine first'
    logger.debug('checking if table <{}> exists'.format(tablename))

    return inspect(engine).has_table(tablename)


def get_table_object(tablename=None, engine=None):
    assert isinstance(tablename, str), 'check tablename type or value'
    assert engine is not None, 'you should instantiate the database engine first'
    logger.debug('returning orm table <{}> object if exists'.format(tablename))

    return Table('ods_{feed}'.format(feed=tablename), MetaData(bind=engine), autoload=True)


def create_table(*columns, tablename=None, engine=None):
    assert isinstance(tablename, str), 'tablename should be string'
    assert engine is not None, 'you should instantiate the database engine first'
    logger.debug('create table <{}> if not exists'.format(tablename))

    table = Table(
        tablename,
        meta,
        *columns
    )
    meta.create_all(engine, checkfirst=True)
    return table


def stmt_insert_update(table, records_to_insert):
    logger.debug('upserting data into table <{}>'.format(table.name))
    with session_instance() as o_session:
        for record in records_to_insert:
            stmt_simple_insert = (
                insert(table).
                values(record)
            )
            record['last_updated'] = datetime.utcnow()
            on_conflict_key_stmt = stmt_simple_insert.on_conflict_do_update(
                constraint=table.primary_key.name,
                set_=record
            )
            o_session.execute(statement=on_conflict_key_stmt)
            o_session.commit()
    logger.debug('upserting data finished with success')

    return True


def stmt_delete(table, key, records_to_delete):
    logger.debug('removing data from table')
    with session_instance() as o_session:
        for record in records_to_delete:
            stmt_simple_delete = (
                delete(table).
                where(table.c.line == record[key])
            )
            o_session.execute(statement=stmt_simple_delete)
            o_session.commit()
    logger.debug('removing data finished with success')

    return True