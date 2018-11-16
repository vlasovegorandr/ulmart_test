import xlrd
import requests
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from time import strftime
import traceback
import json
import sys

engine = create_engine('sqlite:///monitoring.db')
Base = declarative_base()
Session = sessionmaker(bind=engine)

def open_file(path):
    book = xlrd.open_workbook(path)
    sheet = book.sheet_by_index(0)
    for row in range(1, sheet.nrows):
        fetch = sheet.row_values(row, 2)[0]
        if fetch.upper() == 'TRUE':
            yield {
                'label': sheet.row_values(row, 1)[0],
                'url': sheet.row_values(row, 0)[0]
                }

def poll_the_urls(url_gen):
    for item in url_gen:
        url = item.get('url')
        label = item.get('label')
        response = requests.get(url, stream = True)
        if response.status_code == 200:
            yield {
                'timestamp': strftime('%d.%b.%Y %H:%M:%S'),
                'url': url,
                'label': label,
                'status_code': response.status_code,
                'response_time': response.elapsed.total_seconds() * 1000,
                'content_length': None, #response.headers['content-length'],
            }
        else:
            error_info = {
                'url': url,
                'status_code': response.status_code,
                'response_time': response.elapsed.total_seconds() * 1000,
                'content_length': None,
                'timestamp': strftime('%d.%b.%Y %H:%M:%S'),
            }
            with open('404_errors.json', 'a') as file_404:
                json.dump(error_info, file_404, ensure_ascii=False, indent=4)


class Monitoring(Base):
    __tablename__ = 'monitoring'

    id = Column(Integer, primary_key = True)
    timestamp = Column(String)
    url = Column(String)
    label = Column(String)
    response_time = Column(Float)
    status_code = Column(Integer)
    content_length = Column(Integer)


def write_to_db(data):
    for item in data:
        db_input = Monitoring(timestamp=item.get('timestamp'), url=item.get('url'), \
                              label=item.get('label'), response_time=item.get('response_time'), \
                              status_code=item.get('status_code'), content_length=item.get('content_length'))
        session = Session()
        session.add(db_input)
        session.commit()

if __name__ == '__main__':
    #Monitoring.__table__.create(engine)
    try:
        filepath = sys.argv[1]
        write_to_db(poll_the_urls(open_file(filepath)))

    except Exception as ex:
        template = "Exception of type {0}, arguments: \n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        timestamp = strftime('%d.%b.%Y %H:%M:%S')
        json_text = {"timestamp": timestamp,
                     "error": {
                         "exc_type": type(ex).__name__,
                         "exc_value": ex.args,
                         "traceback_info": traceback.format_exc()
                        }
                     }
        with open('exceptions.json', 'w') as file:
            json.dump(json_text, file, ensure_ascii=False, indent=4)
