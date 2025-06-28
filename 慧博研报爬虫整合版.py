# 导入所需的库
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import os

def get_recent_financial_news():
    """
    获取多个财经新闻来源的最近一周新闻，并合并、排序、去重后保存。
    """
    print("开始获取最近一周的财经新闻...")
    
    # 设置时间范围为最近7天
    seven_days_ago = datetime.now() - timedelta(days=7)
    all_news_list = []

    # 1. 财新网 - 内容精选
    try:
        print("\n--- 正在获取财新网新闻 ---")
        stock_news_main_cx_df = ak.stock_news_main_cx()
        # 将 'pub_time' 列转换为 datetime 对象，并处理无效日期
        stock_news_main_cx_df['pub_time'] = pd.to_datetime(stock_news_main_cx_df['pub_time'], errors='coerce')
        # 筛选最近7天的新闻
        cx_recent = stock_news_main_cx_df[stock_news_main_cx_df['pub_time'] >= seven_days_ago].copy()
        # 标准化列名，并移除链接
        cx_recent = cx_recent.rename(columns={'pub_time': '时间', 'tag': '标题'})
        cx_recent['来源'] = '财新网'
        cx_recent = cx_recent[['来源', '时间', '标题']]
        all_news_list.append(cx_recent)
        print(f"成功获取并处理 {len(cx_recent)} 条财新网新闻。")
    except Exception as e:
        print(f"获取财新网新闻失败: {e}")

    # 2. 同花顺财经 - 全球财经直播
    try:
        print("\n--- 正在获取同花顺财经新闻 ---")
        stock_info_global_ths_df = ak.stock_info_global_ths()
        # 将 '发布时间' 列转换为 datetime 对象
        stock_info_global_ths_df['发布时间'] = pd.to_datetime(stock_info_global_ths_df['发布时间'], errors='coerce')
        ths_recent = stock_info_global_ths_df[stock_info_global_ths_df['发布时间'] >= seven_days_ago].copy()
        # 标准化列名，并移除链接
        ths_recent = ths_recent.rename(columns={'发布时间': '时间'})
        ths_recent['来源'] = '同花顺'
        ths_recent = ths_recent[['来源', '时间', '标题']]
        all_news_list.append(ths_recent)
        print(f"成功获取并处理 {len(ths_recent)} 条同花顺新闻。")
    except Exception as e:
        print(f"获取同花顺财经新闻失败: {e}")

    # 3. 财联社 - 电报
    try:
        print("\n--- 正在获取财联社新闻 ---")
        stock_info_global_cls_df = ak.stock_info_global_cls(symbol="全部")
        # --- 核心修正 ---
        # 在合并前，将"发布日期"和"发布时间"都强制转换为字符串类型，避免类型错误
        stock_info_global_cls_df['时间'] = pd.to_datetime(stock_info_global_cls_df['发布日期'].astype(str) + ' ' + stock_info_global_cls_df['发布时间'].astype(str), errors='coerce')
        cls_recent = stock_info_global_cls_df[stock_info_global_cls_df['时间'] >= seven_days_ago].copy()
        # 标准化列名，并移除链接
        cls_recent['来源'] = '财联社'
        cls_recent = cls_recent[['来源', '时间', '标题']]
        all_news_list.append(cls_recent)
        print(f"成功获取并处理 {len(cls_recent)} 条财联社新闻。")
    except Exception as e:
        print(f"获取财联社新闻失败: {e}")

    # 4. 新浪财经 - 证券原创 (循环获取多页以覆盖最近一周)
    try:
        print("\n--- 正在获取新浪财经新闻 (可能需要一些时间) ---")
        sina_all = pd.DataFrame()
        # 循环获取5页，通常足以覆盖最近一周
        for page in range(1, 6): 
            print(f"正在获取第 {page} 页...")
            stock_info_broker_sina_df = ak.stock_info_broker_sina(page=str(page))
            stock_info_broker_sina_df['时间'] = pd.to_datetime(stock_info_broker_sina_df['时间'], errors='coerce')
            sina_all = pd.concat([sina_all, stock_info_broker_sina_df])
            # 如果最后一篇文章的时间已经早于7天前，就停止翻页
            if not sina_all.empty and sina_all['时间'].min() < seven_days_ago:
                break
            time.sleep(1) # 礼貌地等待一下
            
        sina_recent = sina_all[sina_all['时间'] >= seven_days_ago].copy()
        # 标准化列名，并移除链接
        sina_recent = sina_recent.rename(columns={'内容': '标题'})
        sina_recent['来源'] = '新浪财经'
        sina_recent = sina_recent[['来源', '时间', '标题']]
        all_news_list.append(sina_recent)
        print(f"成功获取并处理 {len(sina_recent)} 条新浪财经新闻。")
    except Exception as e:
        print(f"获取新浪财经新闻失败: {e}")

    # 合并所有新闻源的数据
    if not all_news_list:
        print("\n未能从任何来源获取到新闻数据。")
        return
        
    new_df = pd.concat(all_news_list, ignore_index=True)
    
    # 按时间倒序排序
    new_df = new_df.sort_values(by='时间', ascending=False)
    
    # 生成带时间戳的文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"financial_news_last_week_{timestamp}.csv"
    archive_filename = "financial_news_archive.csv"
    
    # 检查是否存在历史数据文件，如果存在则读取并与新数据合并去重
    if os.path.exists(archive_filename):
        try:
            print("\n--- 读取历史数据并进行去重 ---")
            existing_df = pd.read_csv(archive_filename, encoding='utf-8-sig')
            existing_df['时间'] = pd.to_datetime(existing_df['时间'], errors='coerce')
            
            # 合并新旧数据
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # 使用标题和来源进行去重，保留最新的时间记录
            combined_df = combined_df.sort_values('时间', ascending=False)
            combined_df = combined_df.drop_duplicates(subset=['标题', '来源'], keep='first')
            
            # 只保留最近7天的数据
            combined_df = combined_df[combined_df['时间'] >= seven_days_ago]
            
            # 再次按时间倒序排序
            final_df = combined_df.sort_values(by='时间', ascending=False)
            
            print(f"去重前新闻数量: {len(new_df)}")
            print(f"去重后新闻总数量: {len(final_df)}")
        except Exception as e:
            print(f"读取历史数据失败: {e}")
            final_df = new_df
    else:
        final_df = new_df
    
    print(f"\n--- 所有新闻处理完毕 ---")
    print(f"共整理 {len(final_df)} 条最近一周的新闻，正在保存到文件: {filename} 和 {archive_filename}")
    
    # 保存到CSV，使用 utf-8-sig 编码确保Excel能正确显示中文
    final_df.to_csv(filename, index=False, encoding='utf-8-sig')
    final_df.to_csv(archive_filename, index=False, encoding='utf-8-sig')
    
    print("文件保存成功！")
    print("\n最终数据预览:")
    print(final_df.head())

# --- 主程序入口 ---
if __name__ == "__main__":
    get_recent_financial_news()
