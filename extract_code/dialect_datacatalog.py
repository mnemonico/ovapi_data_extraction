from sqlalchemy import Column, Integer, String, Date, DateTime
from datetime import datetime

line_table = {
    'tablename': 'line',
    'columns': ['line', 'linewheelchairaccessible', 'transporttype', 'dataownercode',
                'destinationcode', 'linepublicnumber', 'lineplanningnumber', 'linename',
                'linedirection', 'destinationname50', 'extraction_date'],
    'keys': 'line',
    'ddl': [Column('line', String, primary_key=True),
            Column('linewheelchairaccessible', String),
            Column('transporttype', String),
            Column('dataownercode', String),
            Column('destinationcode', String),
            Column('linepublicnumber', String),
            Column('lineplanningnumber', String),
            Column('linename', String(255)),
            Column('linedirection', Integer),
            Column('destinationname50', String(255)),
            Column('extraction_date', Date),
            Column('last_updated', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow())
            ]
}
