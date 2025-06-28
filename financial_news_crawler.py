#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 导入所需的库
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import os

def get_recent_financial_news():
    """
    获取多个财经新闻来源的最近一周新闻，并与已有数据合并、去重、排序后保存。
    按月保存数据到不同的CSV文件中。
    """
    print("开始获取最近一周的财经新闻...")
    
    # 设置时间范围为最近7天
    seven_days_ago = datetime.now() - timedelta(days=7)
    all_news_list = []

    # 1. 财新网 - 内容精选
    try:
        print("\n--- 正在获取财新网新闻 ---")
        stock_news_main_cx_df = ak.stock_news_main_cx()
        stock_news_main_cx_df['pub_time'] = pd.to_datetime(stock_news_main_cx_df['pub_time'], errors='coerce')
        cx_recent = stock_news_main_cx_df[stock_news_main_cx_df['pub_time'] >= seven_days_ago].copy()
        
        cx_recent = cx_recent.rename(columns={'pub_time': '时间', 'tag': '标题'})
        cx_recent['来源'] = '财新网'
        cx_recent = cx_recent[['来源', '时间', '标题']]
        
        # 过滤空标题
        cx_recent.dropna(subset=['标题'], inplace=True)
        cx_recent = cx_recent[cx_recent['标题'].astype(str).str.strip() != '']
        
        all_news_list.append(cx_recent)
        print(f"成功获取并处理 {len(cx_recent)} 条财新网新闻。")
    except Exception as e:
        print(f"获取财新网新闻失败: {e}")

    # 2. 同花顺财经 - 全球财经直播
    try:
        print("\n--- 正在获取同花顺财经新闻 ---")
        stock_info_global_ths_df = ak.stock_info_global_ths()
        stock_info_global_ths_df['发布时间'] = pd.to_datetime(stock_info_global_ths_df['发布时间'], errors='coerce')
        ths_recent = stock_info_global_ths_df[stock_info_global_ths_df['发布时间'] >= seven_days_ago].copy()

        ths_recent = ths_recent.rename(columns={'发布时间': '时间'})
        ths_recent['来源'] = '同花顺'
        ths_recent = ths_recent[['来源', '时间', '标题']]
        
        # 过滤空标题
        ths_recent.dropna(subset=['标题'], inplace=True)
        ths_recent = ths_recent[ths_recent['标题'].astype(str).str.strip() != '']
        
        all_news_list.append(ths_recent)
        print(f"成功获取并处理 {len(ths_recent)} 条同花顺新闻。")
    except Exception as e:
        print(f"获取同花顺财经新闻失败: {e}")

    # 3. 财联社 - 电报
    try:
        print("\n--- 正在获取财联社新闻 ---")
        stock_info_global_cls_df = ak.stock_info_global_cls(symbol="全部")
        stock_info_global_cls_df['时间'] = pd.to_datetime(stock_info_global_cls_df['发布日期'].astype(str) + ' ' + stock_info_global_cls_df['发布时间'].astype(str), errors='coerce')
        cls_recent = stock_info_global_cls_df[stock_info_global_cls_df['时间'] >= seven_days_ago].copy()

        cls_recent['来源'] = '财联社'
        cls_recent = cls_recent[['来源', '时间', '标题']]
        
        # 过滤空标题
        cls_recent.dropna(subset=['标题'], inplace=True)
        cls_recent = cls_recent[cls_recent['标题'].astype(str).str.strip() != '']
        
        all_news_list.append(cls_recent)
        print(f"成功获取并处理 {len(cls_recent)} 条财联社新闻。")
    except Exception as e:
        print(f"获取财联社新闻失败: {e}")

    # 4. 新浪财经 - 证券原创
    try:
        print("\n--- 正在获取新浪财经新闻 (可能需要一些时间) ---")
        sina_all = pd.DataFrame()
        for page in range(1, 6): 
            print(f"正在获取第 {page} 页...")
            stock_info_broker_sina_df = ak.stock_info_broker_sina(page=str(page))
            
            # --- 核心修正: 指定日期格式 ---
            stock_info_broker_sina_df['时间'] = pd.to_datetime(stock_info_broker_sina_df['时间'], format='%Y年%m月%d日 %H:%M', errors='coerce')
            
            sina_all = pd.concat([sina_all, stock_info_broker_sina_df])
            if not sina_all.empty and sina_all['时间'].min() < seven_days_ago:
                break
            time.sleep(1)
            
        sina_recent = sina_all[sina_all['时间'] >= seven_days_ago].copy()
        
        sina_recent = sina_recent.rename(columns={'内容': '标题'})
        sina_recent['来源'] = '新浪财经'
        sina_recent = sina_recent[['来源', '时间', '标题']]

        # 过滤空标题
        sina_recent.dropna(subset=['标题'], inplace=True)
        sina_recent = sina_recent[sina_recent['标题'].astype(str).str.strip() != '']
        
        all_news_list.append(sina_recent)
        print(f"成功获取并处理 {len(sina_recent)} 条新浪财经新闻。")
    except Exception as e:
        print(f"获取新浪财经新闻失败: {e}")

    # 合并所有新闻源的数据
    if not all_news_list:
        print("\n未能从任何来源获取到新闻数据。")
        return
        
    new_df = pd.concat(all_news_list, ignore_index=True)
    
    # 获取当前年月
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    
    # 创建数据目录
    output_dir = "财经新闻数据"
    os.makedirs(output_dir, exist_ok=True)
    
    # 构建当月文件名
    archive_filename = os.path.join(output_dir, f"financial_news_archive-{current_year}-{current_month:02d}.csv")
    
    # 尝试读取当月的存档文件
    if os.path.exists(archive_filename):
        try:
            print(f"\n--- 正在读取当月存档文件: {archive_filename} ---")
            existing_df = pd.read_csv(archive_filename, encoding='utf-8-sig')
            
            # 确保时间列是日期时间格式
            existing_df['时间'] = pd.to_datetime(existing_df['时间'], errors='coerce')
            
            # 合并新旧数据
            print("合并新旧数据...")
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # 根据标题去重，保留最新的记录
            print("去除重复新闻...")
            final_df = final_df.sort_values('时间', ascending=False).drop_duplicates(subset=['标题'], keep='first')
            
            print(f"合并后共有 {len(final_df)} 条新闻")
        except Exception as e:
            print(f"读取或处理已有文件时出错: {e}")
            print("将只使用新抓取的数据")
            final_df = new_df
    else:
        print(f"\n未找到当月存档文件，将创建新文件: {archive_filename}")
        final_df = new_df
    
    # 按时间倒序排序
    final_df = final_df.sort_values(by='时间', ascending=False)
    
    print(f"\n--- 所有新闻处理完毕 ---")
    print(f"共有 {len(final_df)} 条当月新闻，正在保存到文件: {archive_filename}")
    
    # 保存到CSV，使用 utf-8-sig 编码确保Excel能正确显示中文
    final_df.to_csv(archive_filename, index=False, encoding='utf-8-sig')
    
    print("文件保存成功！")
    print("\n最终数据预览:")
    print(final_df.head())

# --- 主程序入口 ---
if __name__ == "__main__":
    get_recent_financial_news() 