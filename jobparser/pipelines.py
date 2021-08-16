from pymongo import MongoClient
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class JobparserPipeline:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongo_base = client['vacancies']

    def process_item(self, item, spider):
        # if spider.name == 'hhru':
            salary = item['salary_min'].split(' ')
            if salary[0] == 'з/п':
                salary_min, salary_max, currency = None, None, None
            elif salary[2] == 'до':
                salary_min = float(''.join(salary[1].split('\xa0')))
                salary_max = float(''.join(salary[3].split('\xa0')))
                currency = salary[-1]
            elif salary[0] == 'от':
                salary_min = float(''.join(salary[1].split('\xa0')))
                salary_max = None
                currency = salary[-1]
            else:
                salary_min = None
                salary_max = float(''.join(salary[1].split('\xa0')))
                currency = salary[-1]

            item['salary_min'] = salary_min
            item['salary_max'] = salary_max
            item['currency'] = currency

            item['location'] = "".join(item['location'])
            item['company'] = "".join(item['company']).replace("\xa0", " ")

            collection = self.mongo_base['hhru']
            collection.update_one(
                {'url': item['url']},
                {'$set': item},
                upsert=True)

            return item
