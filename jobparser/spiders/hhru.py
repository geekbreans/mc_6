import scrapy
from scrapy.http import HtmlResponse
from jobparser.items import JobparserItem

class HhruSpider(scrapy.Spider):
    class HhruSpider(scrapy.Spider):
        name = 'hhru'
        allowed_domains = ['hh.ru']
        search = "python"
        start_urls = [f'https://izhevsk.hh.ru/search/vacancy?area=&fromSearchLine=true&st=searchVacancy&text={search}']

        def parse(self, response: HtmlResponse):
            next_page = response.xpath("//a[@data-qa='pager-next']/@href").extract_first()
            if next_page:
                yield response.follow(next_page, callback=self.parse)

            links = response.xpath("//a[@data-qa='vacancy-serp__vacancy-title']/@href").extract()
            for link in links:
                yield response.follow(link, callback=self.vacancy_parse)

        def vacancy_parse(self, response: HtmlResponse):
            name = response.xpath('//h1/text()').extract_first()
            salary_min = response.xpath('//p[@class="vacancy-salary"]/span/text()').extract_first()
            location = response.xpath('//a[@data-qa="vacancy-view-link-location"]//text()').extract()
            if not location:
                location = response.xpath('//p[@data-qa="vacancy-view-location"]//text()').extract()
            url = response.url
            company = response.xpath('//a[@data-qa="vacancy-company-name"]//text()').extract()
            yield JobparserItem(name=name, salary_min=salary_min, location=location,
                                url=url, company=company)