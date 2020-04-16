# -*- coding: UTF-8 -*-
import getstart
from multiprocessing import Pool
import pandas as pd
import datetime, re
from lxml import etree
from openpyxl import load_workbook
# from bs4 import BeautifulSoup


EASTMONEY_URL = 'http://fund.eastmoney.com/'
TRANSACTION_FILE = 'E:\\jxin\\OneDrive\\Sync\\TransactionNote.xlsx'
INFORMATION_FILE = 'E:\\jxin\\OneDrive\\Sync\\BasicInformation.xlsx'


# 查询基金类型和起始日期，返回包含类型和日期的字符串元组
def get_type_and_start_date(fund_id):
    search_url = EASTMONEY_URL + fund_id + '.html?spm=search'
    # 获得网页内容；设置网页编码；在天天基金网获得起始日期
    # 用到 requests,re模块
    soup = getstart.geturl_utf8(search_url)
    type_info = re.findall('<td>基金类型：(.*?)</td>', str(soup))
    start_date = re.findall('<td><span class="letterSpace01">成 立 日</span>：(.*?)</td>', str(soup))

    # 以防止输入的编码查不到信息，对无记录的基金代码进行标注，后续方便剔除
    try:
        fund_type = re.compile('>(.*?)<').findall(type_info[0])
    except:
        fund_type = ['未知']
    if start_date == []:
        start_date = ['未知']

    return (fund_type[0], start_date[0])


# 本函数获取单只基金一页的数据，per最高等于20，即最多可以有二十行，返回list
def get_funds_detail_1page_list(url_fund):
    ######定义空列表存储基金信息
    key_list = []
    list_info = []
    ######re.findall,xpath匹配信息
    soup = getstart.geturl_utf8(url_fund)
    records = re.findall('records:(.*?),pages:', str(soup))[0]
    pages = re.findall('pages:(.*?),curpage:', str(soup))[0]
    text_html = re.findall('content:"(.*?)",records:', str(soup))[0]
    #    print(text_html)
    html = etree.HTML(text_html)
    #    print(html)

    # 提取表头元素作为字典的键，共7列
    head_data = html.xpath('//tr/th')
    for head in head_data:
        key_list.append(head.text)

    # 按行提取单元格元素，共20行7列
    tbody_data = html.xpath('//tbody/tr')
    # print(tbody_data, '\n', '===' * 20)
    for tr_data in tbody_data:
        tr_dict = {}
        td_data = tr_data.xpath('td')
        for index, info in enumerate(td_data):
            try:
                # 若接口返回“暂无数据”，则将字典清空
                if info.text == '暂无数据!':
                    tr_dict.clear()
                    break
                else:
                    tr_dict[key_list[index]] = info.text
            except:
                # 若获取到的数据出现异常，则将字典清空，records减1
                tr_dict.clear()
                records -= 1
                break
        if len(tr_dict) > 0:
            list_info.append(tr_dict)

    # print(list_info)
    return (list_info, int(records), int(pages))


# def run_detail2(code, name, url):
#     soup = getstart.geturl_utf8(url)
#     tags = soup.find_all(class_='ui-font-middle ui-color-red ui-num')
#     m1 = tags[3].string
#     y1 = tags[4].string
#     m3 = tags[5].string
#     y3 = tags[6].string
#     m6 = tags[7].string
#     rece = tags[8].string
#     detail = {'代码': code, '名称': name, '近1月': m1, '近3月': m3, '近6月': m6, '近1年': y1, '近3年': y3, '成立来': rece}
#     # print(detail)
#     col2.insert(detail)


# 具体查询并处理基金净值数据，返回单只基金起止时间内的净值列表
def run_get_detail_func(fund_code, start_date, end_date):
    values_list = []
    # 当前页码，从1开始
    page_number = 1
    # 净值总页数
    total_pages = 1
    # 净值总记录数
    record_counts = 0
    # 设置基金的数据查询接口网址，时间为：起始日期前一天（即上次执行程序的前一天）至程序执行的前一天，便于数据合并时进行日期定位
    start_yesterday = start_date + datetime.timedelta(days=-1)
    end_yesterday = end_date + datetime.timedelta(days=-1)

    while page_number <= total_pages:
        url1 = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz'
        url2 = '&code=' + fund_code
        url3 = '&page=' + str(page_number)
        url4 = '&sdate=' + str(start_yesterday)
        url5 = '&edate=' + str(end_yesterday)
        url6 = '&per=20'
        url_fund = url1 + url2 + url3 + url4 + url5 + url6

        # 开始执行查询基金净值数据的函数
        result_tuple = get_funds_detail_1page_list(url_fund)
        # 获取一页净值数据
        values_list.extend(result_tuple[0])
        record_counts = result_tuple[1]
        total_pages = result_tuple[2]
        if page_number == total_pages:
            if record_counts != len(values_list):
                values_list.clear()
        # 处理循环变量
        page_number += 1

    return values_list


# 从交易列表中读取已持有基金代码
def get_holding_funds_id():
    # 读取Excel文件中的交易表，并指定按字符串处理证券代码
    df = pd.read_excel(TRANSACTION_FILE, sheet_name = '交易', converters = {'证券代码':str})
    if df.empty:
        return ([])
    else:
        # 删除证券代码列的nan，并去重
        df_id_array = df['证券代码'].dropna().unique()
        return (df_id_array)


# 对属性列表中不存在记录的基金，从天天基金网的页面中爬取其基本属性
def get_holding_funds_basic(funds_ids):
    global EASTMONEY_URL
    ready_list = []
    # 此版本通过天天基金网的全部基金页面来爬取数据
    # 也可以通过网站提供的接口来获取基金代码、基金名称简称的集合 http://fund.eastmoney.com/js/fundcode_search.js
    soup = getstart.geturl_gbk(EASTMONEY_URL + 'allfund.html')
    tags = soup.select('.num_right > li')
    for tag in tags:
        if tag.a is None:
            continue
        else:
            content = tag.a.text
            code = re.findall(r'\d+', content)[0]
            # print(code)
            if code not in funds_ids:
                continue
            name = content.split('）')[1]
            # print(name)
            QUERY_URL = tag.a['href']
            # print(content)
            attr_tuple = get_type_and_start_date(code)
            if len(str(attr_tuple[1])) <= 5:
                continue
            else:
                content_dict = {'code': code, 'name': name, 'type': attr_tuple[0], 'url': QUERY_URL, 'found': attr_tuple[1], 'update':''}
                # print (content_dict)
                ready_list.append(content_dict)

            # time.sleep(0.1)
    return ready_list


# 获取持有基金净值数据执行函数
def get_holding_funds_details(holding_funds_ids):
    # 表示程序执行状态
    flag = False
    # 第1步：通过属性表，获得基金净值数据的查询地址
    # 读取Excel文件中的属性表，并指定按字符串处理证券代码
    df = pd.read_excel(INFORMATION_FILE, sheet_name='属性', converters={'证券代码': str})
    # 准备待写入的ExcelWriter，并与工作簿和工作表相关联
    writer = pd.ExcelWriter(INFORMATION_FILE, date_format='YYYY-MM-DD', mode='a', engine='openpyxl')
    book = load_workbook(INFORMATION_FILE)
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    print('---------数据加载完毕，开始检测历史基金数据是否更新---------')

    # 第2步：根据已经获得的基金代码，逐一查询每只基金的净值数据
    # 先查询属性表中原有基金的净值数据，起始日期设置为上次更新当天（上次更新当天可能未取到基金净值数据）
    origin_funds_basic = df[['证券代码', '资产名称', '更新时间']]
    if not origin_funds_basic.empty:
        for row in origin_funds_basic.iterrows():
            code = str(row[1]['证券代码'])
            str_start = str(row[1]['更新时间'])
            start = datetime.datetime.strptime(str_start,'%Y-%m-%d').date()
            end = datetime.datetime.today().date()
            values_list = run_get_detail_func(code, start, end)
            # 跳到第4步：将返回的非空结果写入Excel净值表中
            if values_list:
                # 读取该基金的历史净值信息
                fund_values = pd.read_excel(INFORMATION_FILE, sheet_name = code)
                # 将字典列表转换为DataFrame
                new_fund_values = pd.DataFrame(values_list)
                # 指定字段顺序
                order = ['净值日期', '单位净值', '累计净值', '日增长率', '申购状态', '赎回状态', '分红送配']
                new_fund_values = new_fund_values[order]
                # 处理nan单元格
                new_fund_values.fillna('', inplace=True)
                # 输出，将新获取的基金净值数据在前端与历史净值信息拼接后，整体写入Excel
                fund_values = pd.concat(new_fund_values, fund_values, ignore_index=False)
                fund_values.to_excel(writer, sheet_name=code, encoding='utf-8', index=False)
                # 保存表格
                writer.save()
                print('%s 基金净值获取成功，数据已更新---------' % str(row[1]['资产名称']))

                # 第5步：更新Excel属性表里的更新时间
                # 将基金属性表中的更新时间相应修改为today
                fund_basic = df.loc[df['证券代码'] == code]
                fund_basic['更新时间'] = str(datetime.datetime.today().date())
                # 更新时间写入Excel属性表
                fund_basic.to_excel(writer, sheet_name='属性', encoding='utf-8', index=False, header=False, startrow=(df['证券代码'] == code))
                writer.save()
                flag = True
            else:
                print('待查询的基金暂未更新净值数据，历史基金处理完毕…………')
    else:
        print('未读取到历史基金数据，历史基金处理完毕…………')

    # 第3步：对于属性表中没有记录的新增基金，从网络中获取其基本信息，再查询其净值数据
    print('---------即将开始检测是否存在新持有基金---------')
    # 删除证券代码列的nan，并去重
    basic_funds_ids = origin_funds_basic['证券代码'].dropna()
    # 对比得到尚无属性数据的基金
    different_funds = set(holding_funds_ids).difference(set(basic_funds_ids))
    if different_funds:
        # 获取新增基金基本信息
        new_funds_basic_list = get_holding_funds_basic(different_funds)
        print('---------发现新近持有的基金，开始查询新持有基金净值数据---------')
        # 查询新增基金的净值信息，默认从基金成立日开始查询
        if new_funds_basic_list:
            for new_fund in new_funds_basic_list:
                code = new_fund['code']
                try:
                    start = datetime.datetime.strptime(new_fund['found'],'%Y-%m-%d').date()
                except:
                    # 若基金日期不存在，则将start_date设为今天
                    start = datetime.datetime.today().date()
                end = datetime.datetime.today().date()
                values_list = run_get_detail_func(code, start, end)

                # 第4步：将返回的非空结果写入Excel净值表中
                if values_list:
                    # 将字典列表转换为DataFrame
                    new_fund_values = pd.DataFrame(values_list)
                    # 指定字段顺序
                    order = ['净值日期', '单位净值', '累计净值', '日增长率', '申购状态', '赎回状态', '分红送配']
                    new_fund_values = new_fund_values[order]
                    # 处理nan单元格
                    new_fund_values.fillna('', inplace=True)
                    # 输出，由于是新增基金，因此将dataframe的数据整体写入Excel
                    new_fund_values.to_excel(writer, sheet_name=code, encoding='utf-8', index=False)
                    # 保存表格
                    writer.save()
                    # 修改基金属性表中的更新时间为today
                    new_fund['update'] = str(datetime.datetime.today().date())
                    print('%s 基金净值获取成功，数据已保存---------' % str(new_fund['name']))

                    # 第5步：将新获取的基金属性数据写入Excel属性表中
                    # 将字典列表转换为DataFrame
                    new_fund_basic = pd.DataFrame(new_fund, index=[0])
                    # 指定字段顺序
                    order = ['code', 'name', 'type', 'url', 'found', 'update']
                    new_fund_basic = new_fund_basic[order]
                    # 将列名替换为中文
                    columns_map = {
                        'code': '证券代码',
                        'name': '资产名称',
                        'type': '资产类别',
                        'url': '信息地址',
                        'found': '成立时间',
                        'update': '更新时间'
                    }
                    new_fund_basic.rename(columns=columns_map, inplace=True)
                    # 替换空单元格
                    new_fund_basic.fillna(' ', inplace=True)
                    # 获取Excel属性表当前的行数
                    df_rows = df.shape[0]
                    # 输出，采用后端追加的方式
                    new_fund_basic.to_excel(writer, sheet_name='属性', encoding='utf-8', index=False, header = False, startrow = df_rows + 1)
                    # 同步修改dataframe，在df中增加一行，df_rows加1，控制下次写入的行位置
                    df = df.append(new_fund_basic, ignore_index=True)
                    writer.save()
                    flag = True
                else:
                    print('新持有的基金尚未发布净值数据，查询执行完毕…………')
        else:
            print('新持有的基金尚未成立，查询执行完毕…………')
    else:
        print('未发现新持有的基金，查询执行完毕…………')

    return flag


if __name__ == '__main__':
    print('---------正在从交易表中读取待查询的基金信息---------')
    fundsIdArray = get_holding_funds_id()
    if len(fundsIdArray) > 0:
        if get_holding_funds_details(fundsIdArray):
            print('数据获取完毕，更新时间：%s' % datetime.date.today())
        else:
            print('未获取到新数据，请确认是否存在错误…………')
    else:
        print('未读取到基金信息，请检查交易表是否存在错误…………')