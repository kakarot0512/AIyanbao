#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 导入所需的库
import time
import csv
import re
import os
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# ===== 投研资讯网爬虫函数 =====
def scrape_hibor_multi_page(driver, start_page=1, end_page=20):
    """
    使用 undetected-chromedriver 自动抓取慧博投研资讯网的多个列表页。

    Args:
        driver: Selenium的浏览器驱动实例。
        start_page (int): 开始的页码。
        end_page (int): 结束的页码。
        
    Returns:
        list: 包含所有抓取到的研报信息的列表。
    """
    all_reports = []
    base_url = "https://www.hibor.com.cn/microns_1_{page_num}.html"

    for page_num in range(start_page, end_page + 1):
        url = base_url.format(page_num=page_num)
        print(f"--- 开始处理投研资讯网第 {page_num} 页 --- URL: {url}")

        try:
            driver.get(url)
            
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "tableList"))
            )

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            table = soup.find('table', id='tableList')
            if not table:
                print(f"在第 {page_num} 页未找到ID为 'tableList' 的表格，跳过此页。")
                continue

            rows = table.select('tbody > tr')
            page_reports_count = 0
            
            for i in range(0, len(rows), 4):
                if i+2 >= len(rows):
                    continue
                    
                title_row, summary_row, meta_row = rows[i], rows[i+1], rows[i+2]

                if not (title_row and summary_row and meta_row):
                    continue

                title_element = title_row.select_one('a[href^="/data/"]')
                full_title = title_element.text.strip() if title_element else 'N/A'
                
                summary_cell = summary_row.find('td')
                summary = 'N/A'
                if summary_cell:
                    summary_clone = summary_cell
                    detail_link = summary_clone.find('a')
                    if detail_link: detail_link.decompose()
                    summary = summary_clone.text.strip()
                
                meta_cell = meta_row.find('td')
                author, rating, report_date, pages, sharer = ('N/A',) * 5
                if meta_cell:
                    for span in meta_cell.find_all('span', recursive=False):
                        text = span.text.strip()
                        #if '作者：' in text:
                        #    author_tag = span.find('a')
                        #    if author_tag: author = author_tag.text.strip()
                        elif '评级：' in text:
                            rating_tag = span.find('label')
                            if rating_tag: rating = rating_tag.text.strip()
                        #elif '页数：' in text:
                        #    match = re.search(r'(\d+)', text)
                        #    if match: pages = match.group(1)
                        #elif '分享者：' in text:
                        #    sharer = text.replace('分享者：', '').strip()
                        else:
                            date_match = re.search(r'\d{4}-\d{2}-\d{2}', text)
                            if date_match: report_date = date_match.group(0)
                
                all_reports.append({
                    #"分类": "投研资讯",
                    "研报标题": full_title,
                    "摘要": summary,
                    #"作者": author,
                    "评级": rating,
                    #"页数": pages,
                    "日期": report_date,
                    #"分享者": sharer,
                    #"来源页": page_num,
                    #"抓取时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                page_reports_count += 1
            
            print(f"在投研资讯网第 {page_num} 页成功抓取 {page_reports_count} 条数据。")

        except Exception as e:
            print(f"处理投研资讯网第 {page_num} 页时发生错误: {e}")
            continue
    
    return all_reports

# ===== 最新买入研报爬虫函数 =====
def scrape_latest_buy_reports(driver, start_page=1, end_page=10):
    """
    抓取慧博"最新买入"页面的研报。

    Args:
        driver: Selenium的浏览器驱动实例。
        start_page (int): 开始的页码。
        end_page (int): 结束的页码。
        
    Returns:
        list: 包含所有抓取到的研报信息的列表。
    """
    all_reports = []
    base_url = "https://www.hibor.com.cn/rightmore_4_{page_num}.html"

    for page_num in range(start_page, end_page + 1):
        url = base_url.format(page_num=page_num)
        print(f"--- 开始处理最新买入第 {page_num} 页 --- URL: {url}")

        try:
            driver.get(url)
            
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "rightmore-result"))
            )

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            table = soup.find('table', class_='rightmore-result')
            if not table:
                print(f"在第 {page_num} 页未找到 class 为 'rightmore-result' 的表格，跳过此页。")
                continue

            rows = table.find_all('tr')[1:] 
            page_reports_count = 0
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 5:
                    continue

                title_element = cells[1].find('a')
                title = title_element.get('title', '').strip() if title_element else 'N/A'
                if not title:
                    title = title_element.text.strip() if title_element else 'N/A'

                report_type = cells[2].text.strip()
                rating = cells[3].text.strip()
                date = cells[4].text.strip()

                all_reports.append({
                    #"分类": "最新买入",
                    "研报标题": title,
                    #"类型": report_type,
                    "评级": rating,
                    "分享时间": date,
                    #"来源页": page_num,
                    #"抓取时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                page_reports_count += 1
            
            print(f"在最新买入第 {page_num} 页成功抓取 {page_reports_count} 条数据。")

        except Exception as e:
            print(f"处理最新买入第 {page_num} 页时发生错误: {e}")
            continue
            
    return all_reports

# ===== 热门研报爬虫函数 =====
def scrape_hibor_list_page(driver, category_id, category_name, start_page, end_page):
    """
    一个通用的函数，用于抓取慧博热门列表页。

    Args:
        driver: Selenium的浏览器驱动实例。
        category_id (int): 栏目ID (0 for 今日热门, 1 for 本周热门, etc.)。
        category_name (str): 栏目名称，用于在数据中标记。
        start_page (int): 开始的页码。
        end_page (int): 结束的页码。
    
    Returns:
        list: 包含该分类下所有抓取到的研报信息的列表。
    """
    category_reports = []
    base_url = f"https://www.hibor.com.cn/rightmore_{category_id}_{{page_num}}.html"

    for page_num in range(start_page, end_page + 1):
        url = base_url.format(page_num=page_num)
        print(f"--- 开始处理 {category_name} 第 {page_num} 页 --- URL: {url}")

        try:
            driver.get(url)
            
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "rightmore-result"))
            )

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            table = soup.find('table', class_='rightmore-result')
            if not table:
                print(f"在第 {page_num} 页未找到 class 为 'rightmore-result' 的表格，跳过此页。")
                continue

            rows = table.find_all('tr')[1:] 
            page_reports_count = 0
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 5:
                    continue

                title_element = cells[1].find('a')
                title = title_element.get('title', '').strip() if title_element else 'N/A'
                if not title:
                    title = title_element.text.strip() if title_element else 'N/A'

                report_type = cells[2].text.strip()
                rating = cells[3].text.strip()
                date = cells[4].text.strip()

                category_reports.append({
                    #"分类": category_name,
                    "研报标题": title,
                    #"类型": report_type,
                    "评级": rating,
                    "分享时间": date,
                    #"来源页": page_num,
                    #"抓取时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                page_reports_count += 1
            
            print(f"在 {category_name} 第 {page_num} 页成功抓取 {page_reports_count} 条数据。")

        except Exception as e:
            print(f"处理 {category_name} 第 {page_num} 页时发生错误: {e}")
            continue
            
    return category_reports

# ===== 行业分析研报爬虫函数 =====
def scrape_category_page(driver, category_name, category_id, start_page, end_page):
    """
    一个通用的函数，用于抓取慧博"公司调研"或"行业分析"等使用相同布局的页面。

    Args:
        driver: Selenium的浏览器驱动实例。
        category_name (str): 栏目名称，用于在数据中标记。
        category_id (int): 栏目ID (e.g., 2 for 行业分析, 4 for 投资策略)。
        start_page (int): 开始的页码。
        end_page (int): 结束的页码。
        
    Returns:
        list: 包含该分类下所有抓取到的研报信息的列表。
    """
    category_reports = []
    base_url = f"https://www.hibor.com.cn/microns_{category_id}_{{page_num}}.html"

    for page_num in range(start_page, end_page + 1):
        url = base_url.format(page_num=page_num)
        print(f"--- 开始处理 {category_name} 第 {page_num} 页 --- URL: {url}")

        try:
            driver.get(url)
            
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "tableList"))
            )

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            table = soup.find('table', id='tableList')
            if not table:
                print(f"在第 {page_num} 页未找到ID为 'tableList' 的表格，跳过此页。")
                continue

            rows = table.select('tbody > tr')
            page_reports_count = 0
            
            for i in range(0, len(rows), 4):
                if i+2 >= len(rows):
                    continue
                    
                title_row = rows[i]
                summary_row = rows[i + 1]
                meta_row = rows[i + 2]

                if not (title_row and summary_row and meta_row):
                    continue

                title_element = title_row.select_one('a[href^="/data/"]')
                full_title = title_element.text.strip() if title_element else 'N/A'
                
                summary_cell = summary_row.find('td')
                summary = 'N/A'
                if summary_cell:
                    summary_clone = summary_cell
                    detail_link = summary_clone.find('a')
                    if detail_link:
                        detail_link.decompose()
                    summary = summary_clone.text.strip()
                
                meta_cell = meta_row.find('td')
                author, rating, report_date, pages, sharer = ('N/A',) * 5
                if meta_cell:
                    for span in meta_cell.find_all('span', recursive=False):
                        text = span.text.strip()
                        if '作者：' in text:
                            author_tag = span.find('a')
                            if author_tag: author = author_tag.text.strip()
                        elif '评级：' in text:
                            rating_tag = span.find('label')
                            if rating_tag: rating = rating_tag.text.strip()
                        elif '页数：' in text:
                            match = re.search(r'(\d+)', text)
                            if match: pages = match.group(1)
                        elif '分享者：' in text:
                            sharer = text.replace('分享者：', '').strip()
                        else:
                            date_match = re.search(r'\d{4}-\d{2}-\d{2}', text)
                            if date_match: report_date = date_match.group(0)
                
                category_reports.append({
                    #"分类": category_name,
                    "研报标题": full_title,
                    "摘要": summary,
                    #"作者": author,
                    "评级": rating,
                    #"页数": pages,
                    "日期": report_date,
                    #"分享者": sharer,
                    #"来源页": page_num,
                    #"抓取时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                page_reports_count += 1
            
            print(f"在 {category_name} 第 {page_num} 页成功抓取 {page_reports_count} 条数据。")

        except Exception as e:
            print(f"处理 {category_name} 第 {page_num} 页时发生错误: {e}")
            continue
            
    return category_reports

# ===== 数据去重和保存函数 =====
def deduplicate_and_save_by_week(all_data):
    """
    对抓取的数据进行去重，并按周保存为不同的CSV文件。
    
    Args:
        all_data (list): 包含所有抓取数据的列表。
    """
    if not all_data:
        print("没有数据需要保存")
        return
        
    # 转换为DataFrame以便于处理
    df = pd.DataFrame(all_data)
    
    # 创建输出目录
    output_dir = "研报数据"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 去重 - 基于研报标题和分类
    print(f"去重前数据量: {len(df)}")
    df = df.drop_duplicates(subset=['分类', '研报标题'])
    print(f"去重后数据量: {len(df)}")
    
    # 获取当前日期
    today = datetime.now()
    
    # 计算本周的开始日期（周一）和结束日期（周日）
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    
    # 格式化日期为字符串
    week_str = f"{start_of_week.strftime('%Y%m%d')}-{end_of_week.strftime('%Y%m%d')}"
    
    # 构建文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = os.path.join(output_dir, f"慧博研报_第{today.isocalendar()[1]}周_{week_str}_{timestamp}.csv")
    
    # 保存到CSV
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"数据已保存至: {filename}")
    
    # 同时保存一个当前最新版本的文件（方便其他程序引用）
    latest_file = os.path.join(output_dir, "慧博研报_最新数据.csv")
    df.to_csv(latest_file, index=False, encoding='utf-8-sig')
    print(f"最新数据已保存至: {latest_file}")

# ===== 主程序入口 =====
def main():
    """主程序入口函数"""
    all_combined_data = []
    
    print("正在设置浏览器驱动...")
    options = uc.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--log-level=3')
    options.add_argument('--disable-gpu')

    driver = None
    try:
        print("检测到浏览器版本，正在匹配对应驱动...")
        driver = uc.Chrome(options=options, use_subprocess=True)
        driver.implicitly_wait(5) 

        # --- 任务1: 抓取投研资讯网 ---
        print("\n=== 开始抓取投研资讯网数据 ===")
        info_data = scrape_hibor_multi_page(driver, 1, 5)
        all_combined_data.extend(info_data)
        
        print("\n--- 投研资讯网抓取完成，暂停2秒 ---\n")
        time.sleep(2)
        
        # --- 任务2: 抓取最新买入 ---
        print("\n=== 开始抓取最新买入数据 ===")
        buy_data = scrape_latest_buy_reports(driver, 1, 5)
        all_combined_data.extend(buy_data)
        
        print("\n--- 最新买入抓取完成，暂停2秒 ---\n")
        time.sleep(2)

        # --- 任务3: 抓取热门研报 ---
        print("\n=== 开始抓取热门研报数据 ===")
        # 抓取"今日热门"
        today_hot_data = scrape_hibor_list_page(driver, 0, "今日热门", 1, 3)
        all_combined_data.extend(today_hot_data)
        
        print("\n--- '今日热门' 抓取完成，暂停2秒 ---\n")
        time.sleep(2)

        # 抓取"本周热门"
        week_hot_data = scrape_hibor_list_page(driver, 1, "本周热门", 1, 3)
        all_combined_data.extend(week_hot_data)
        
        print("\n--- '本周热门' 抓取完成，暂停2秒 ---\n")
        time.sleep(2)
        
        # 抓取"本月热门"
        month_hot_data = scrape_hibor_list_page(driver, 6, "本月热门", 1, 3)
        all_combined_data.extend(month_hot_data)
        
        print("\n--- '本月热门' 抓取完成，暂停2秒 ---\n")
        time.sleep(2)
        
        # --- 任务4: 抓取行业分析 ---
        print("\n=== 开始抓取行业分析数据 ===")
        # 抓取"行业分析"
        industry_data = scrape_category_page(driver, "行业分析", 2, 1, 3)
        all_combined_data.extend(industry_data)
        
        print("\n--- '行业分析' 抓取完成，暂停2秒 ---\n")
        time.sleep(2)

        # 抓取"投资策略"
        strategy_data = scrape_category_page(driver, "投资策略", 4, 1, 3)
        all_combined_data.extend(strategy_data)

    except Exception as e:
        print(f"程序主流程发生错误: {e}")
    finally:
        if driver:
            driver.quit()
        print("\n--- 所有页面处理完毕 ---")

    # 数据去重和保存
    deduplicate_and_save_by_week(all_combined_data)

if __name__ == "__main__":
    main()
