from pathlib import Path
import json
import asyncio
from itertools import chain
import time

import aiohttp
import requests
from lxml import html


# search query
cont_type = "Конкурсы научных проектов по областям знания, включенным в классификатор РФФИ (группа \"а\")"
cont = "А"
chosen_year = "2018"


URL = "http://search.rfbr.ru/index.php"
SQL_URL = "http://search.rfbr.ru/set_sql.php"
PAGE_URL = "http://search.rfbr.ru/show_page.php"
PROJECT_URL = "http://search.rfbr.ru/show_project.php"
DUMP_FILE = Path("contest_json_data.json")
HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    # "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Cookie": "PHPSESSID=c4e9fa7dda39ea6f19091492351376b3",
    "Host": "search.rfbr.ru",
    "Origin": "http://search.rfbr.ru",
    "Pragma": "no-cache",
    "Referer": "http://search.rfbr.ru/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}
loop = asyncio.get_event_loop()
session = aiohttp.ClientSession(headers=HEADERS, loop=loop)


async def get_html(url, query=None, data=None):
    if data:
        async with session.post(url, params=query, data=data) as resp:
            text = await resp.text(encoding="utf-8")
    else:
        async with session.get(url, params=query) as resp:
            text = await resp.text(encoding="utf-8")

    assert resp.status == 200
    parsed = html.fromstring(text)
    return parsed


def parse_options(tree, xpath):
    container = tree.xpath(xpath)

    res = {}
    for option in container:
        if not option.text:
            continue

        res[option.get("value")] = {"name": option.text}

    return res


async def get_contest_types():
    parsed = await get_html(URL)
    types = parse_options(parsed, ".//select[@id='conquest_type']/option")
    return types


async def get_contests(contest_type_index):
    data = {
        "sfilter": 1,
        "conquest_type": contest_type_index,
        "status": 1,
    }
    parsed = await get_html(URL, data=data)
    contests = parse_options(parsed, ".//select[@id='conquest_name']/option")
    return contests


async def get_years(contest_type_index, contest_index):
    data = {
        "sfilter": 1,
        "conquest_type": contest_type_index,
        "conquest_name_id": contest_index,
        "status": 1,
    }
    parsed = await get_html(URL, data=data)
    years = parse_options(parsed, ".//select[@id='conquest_id']/option")
    return years


async def get_fields(contest_type_id, contest_id, year):
    data = {
        "sfilter": 1,
        "conquest_type": contest_type_id,
        "conquest_name_id": contest_id,
        "conquest_id": year,
        "status": 1,
    }
    parsed = await get_html(URL, data=data)
    fields = parse_options(parsed, ".//select[@id='main_fok_id']/option")
    return fields


async def get_classifiers(contest_type_id, contest_id, year, clsf_id):
    data = {
        "sfilter": 1,
        "conquest_type": contest_type_id,
        "conquest_name_id": contest_id,
        "conquest_id": year,
        "main_fok_id": clsf_id,
        "status": 1,
    }
    parsed = await get_html(URL, data=data)
    clsfs = parse_options(parsed, ".//select[@id='fok_id']/option")
    return clsfs


async def set_sql(contest_type_id, contest_id, year, clsf_id, fok_id):
    data = {
        "sfilter": 1,
        "keyw": "",
        "conquest_type": contest_type_id,
        "conquest_name_id": contest_id,
        "conquest_id": year,
        "main_fok_id": clsf_id,
        "fok_id": fok_id,
        "status": 'true',
    }
    async with session.post(SQL_URL, data=data) as resp:
        return


async def get_page(page_num):
    print(f"\t\tЗагрузка страницы {page_num}")
    data = {"page": page_num}
    parsed = await get_html(PAGE_URL, data=data)
    trs = parsed.xpath(".//tr")

    projects = []
    for tr in trs[1:-1]:
        tds = tr.xpath(".//td")

        projects.append({
            "id": tds[1].xpath(".//a")[0].text,
            "fio": tds[2].text,
            "project_name": tds[3].text,
            "status": tds[4].text,
        })
    pages = [a.text for a in trs[-1].xpath(".//span/a")]

    return projects, pages


async def get_project(p_id):
    print(f"\t\tЗагрузка проекта {p_id}")
    data = {"regnumber": p_id}
    parsed = await get_html(PROJECT_URL, data=data)
    trs = parsed.xpath(".//tr")

    project_data = {}
    for tr in trs:
        td_key, td_val = tr.xpath(".//td")
        project_data[td_key.text] = td_val.text

    return project_data


async def main():
    contest_types = await get_contest_types()
    for contest_type_id, c_type in contest_types.items():
        if c_type["name"] != cont_type:
            continue
        print(f"Тип конкурса: {cont_type}")
        contests = await get_contests(contest_type_id)
        contest_types[contest_type_id]["contests"] = contests

        for contest_id, c in contests.items():
            if c["name"] != cont:
                continue
            print(f"Конкурс: {cont}")
            years = await get_years(contest_type_id, contest_id)
            contest_types[contest_type_id]["contests"][contest_id]["years"] = years

            for year, val in years.items():
                if val["name"] != chosen_year:
                    continue
                print(f"Год: {chosen_year}")
                fields = await get_fields(contest_type_id, contest_id, year)
                contest_types[contest_type_id]["contests"][contest_id]["years"][year]["fields"] = fields

                for field_id, field in fields.items():
                    print(f"Область знаний: {field['name']}")
                    classifiers = await get_classifiers(contest_type_id, contest_id, year, field_id)
                    contest_types[contest_type_id]["contests"][contest_id]["years"][year]["fields"][field_id]["classifiers"] = classifiers

                    for classifier, cls_code in classifiers.items():
                        print(f"\tКод классификатора: {cls_code['name']}")

                        # collect project ids
                        await set_sql(contest_type_id, contest_id, year, field_id, classifier)

                        short_projects, pages = await get_page(1)
                        try:
                            left_projects = await asyncio.gather(*[get_page(page_num=i) for i in pages], return_exceptions=False, loop=loop)
                        except aiohttp.client_exceptions.ServerDisconnectedError:
                            print("\t===== server disconnected us >:[ =====")
                            time.sleep(1)
                            left_projects = await asyncio.gather(*[get_page(page_num=i) for i in pages], return_exceptions=False, loop=loop)

                        for p, _ in left_projects:
                            short_projects.extend(p)
                        print(f"\t\tСтраницы: {['1'] + pages}")

                        # fetch full data from project ids
                        # This method is too fast for the website.
                        # try:
                        #     projects = await asyncio.gather(*[get_project(short_proj["id"]) for short_proj in short_projects], return_exceptions=False, loop=loop)
                        # except aiohttp.client_exceptions.ServerDisconnectedError:
                        #     print("\t===== server disconnected us >:[ =====")
                        #     time.sleep(1)
                        #     projects = await asyncio.gather(*[get_project(short_proj["id"]) for short_proj in short_projects], return_exceptions=False, loop=loop)
                        
                        # Slower method:
                        projects = []
                        for short_proj in short_projects:
                            projects.append(await get_project(short_proj["id"]))

                        contest_types[contest_type_id]["contests"][contest_id]["years"][year]["fields"][field_id]["classifiers"][classifier]["projects"] = projects

                        json.dump(contest_types, DUMP_FILE.open(mode="w", encoding="utf-8"), indent=2, ensure_ascii=False)
    await session.close()


if __name__ == "__main__":
    loop.run_until_complete(main())
